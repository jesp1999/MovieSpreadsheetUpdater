import os
import sqlite3
from typing import Optional, Literal, get_args

import dotenv
import gspread
import pickle
import requests
from flask import Flask, request
from oauth2client.service_account import ServiceAccountCredentials

MEDIA_TYPE = Literal['movie', 'show']
SQL_INSERT_CHUNK_SIZE = 100
ALLOWED_UPSERT_REQUEST_PARAMS = ('title', 'year', 'years', 'status', 'rating', 'watchdate', 'firstwatchdate', 'lastwatchdate', 'watchedwith', 'comments')

app = Flask(__name__)


@app.route('/q/<media_type>', methods=['GET'])
def get_media(media_type: str):
    """
    Endpoint for fetching media entries from the database.
    """
    if media_type not in get_args(MEDIA_TYPE):
        return '<p>Page not found</p>', 404
    media_type: MEDIA_TYPE

    genre = request.args.get('genre')
    num = request.args.get('num')
    status = request.args.get('status')
    if status not in ('Watched', 'Dropped', 'Plan to Watch', 'In Progress', None):
        return '<p>Provided status is invalid</p>', 400
    length = request.args.get('length')
    sort_by = request.args.get('sort').lower() if 'sort' in request.args else None
    if sort_by not in (None, 'length', 'random', 'releasedate', 'criticsrating', 'myrating', 'watchdate'):
        return '<p>Provided sort is invalid</p>', 400
    if sort_by == 'random':
        sort_by = 'RANDOM()'
    order = request.args.get('order')
    if order not in (None, 'asc', 'desc'):
        return '<p>Provided order is invalid</p>', 400


    conn = sqlite3.connect('movie_data.db')
    cursor = conn.cursor()

    q = 'SELECT title, releaseDate FROM movie ' if media_type == 'movie' else 'SELECT title, airingDates FROM show '
    if genre or status or length:
        q += ' WHERE '
    if genre:
        q += ' genres LIKE "%' + genre.lower() + '%" '
    if genre and (status or length):
        q += ' AND '
    if status:
        q += f" status = '" + status + "' "
    if status and length:
        q += ' AND '
    if length:
        q += f" length < {length} "
    if sort_by:
        q += f' ORDER BY {sort_by} '
        if order:
            q += f' {order}'
    if num:
        q += ' LIMIT ' + num

    print(q)

    results = cursor.execute(q).fetchall()
    titles = [f'{r[0]} ({r[1]})' for r in results]

    return '<p>' + '</p>\n<p>'.join(titles) + '</p>', 200


@app.route('/q/<media_type>', methods=['POST'])
def post_media(media_type: str):
    """
    Endpoint for inserting a new media entry into the database. Will fail if a record for the entry already exists.
    Will fetch from IMDB unless the 'noimdb' parameter is present in the request.
    """
    if media_type not in get_args(MEDIA_TYPE):
        return '<p>Page not found</p>', 404
    media_type: MEDIA_TYPE
    imdb = 'noimdb' not in request.form
    return upsert_media(media_type=media_type, insert=True, replace=False, imdb_fetch=imdb)


@app.route('/q/<media_type>', methods=['DELETE'])
def delete_media(media_type: str):
    if media_type not in get_args(MEDIA_TYPE):
        return '<p>Page not found</p>', 404
    media_type: MEDIA_TYPE
    if any(p not in ('title', 'year') for p in request.form.keys()):
        return '<p>Invalid request parameters supplied</p>', 400
    title = request.form.get('title')
    release_year = request.form.get('year')
    q = 'DELETE FROM movie WHERE title = ?'
    p = [title]
    if release_year:
        q += ' AND release_year = ?'
        p.append(release_year)
    conn = sqlite3.connect('movie_data.db')
    cursor = conn.cursor()
    cursor.execute(q, p)
    conn.commit()
    return '<p>Success</p>', 200


@app.route('/q/<media_type>', methods=['PUT'])
def put_media(media_type: str):
    """
    Endpoint for updating a media entry in the database. Will not fail if a record for the entry already exists,
    instead will replace that record.
    Will fetch from IMDB unless the 'noimdb' parameter is present in the request.
    Intended to be used for in-place replacement of existing records.
    """
    if media_type not in get_args(MEDIA_TYPE):
        return '<p>Page not found</p>', 404
    media_type: MEDIA_TYPE
    imdb = 'noimdb' not in request.form
    return upsert_media(media_type=media_type, insert=True, replace=True, imdb_fetch=imdb)


@app.route('/q/<media_type>', methods=['PATCH'])
def patch_media(media_type: str):
    """
    Endpoint for updating a media entry in the database. Will not fail if a record for the entry already exists,
    Intended to be used for updating single fields in a record.
    """
    if media_type not in get_args(MEDIA_TYPE):
        return '<p>Page not found</p>', 404
    media_type: MEDIA_TYPE
    # TODO: Implement changing watch status, rating, etc
    return upsert_media(media_type=media_type, insert=True, replace=True, imdb_fetch=False)


@app.route('/backfill/omdb/<media_type>', methods=['POST'])
def backfill_omdb(media_type: str):
    """
    Endpoint for backfilling missing data in the database from the OMDB API. Will fetch data for a number of entries
    specified by the `num` parameter in the request.
    """
    if media_type not in get_args(MEDIA_TYPE):
        return '<p>Page not found</p>', 404
    media_type: MEDIA_TYPE
    num = int(request.args.get('num')) if 'num' in request.args else 10
    backfill_from_omdb(media_type, num)
    return '<p>Success</p>', 200


def upsert_media(media_type: MEDIA_TYPE, insert: bool = False, replace: bool = False, imdb_fetch: bool = False):
    if any(p not in ALLOWED_UPSERT_REQUEST_PARAMS for p in request.form.keys()):
        return '<p>Invalid request parameters supplied', 400

    # Identifying params for a movie
    title = request.form.get('title')
    aired = request.form.get('year') or request.form.get('years')

    # My params for a movie
    status = request.form.get('status') or 'Plan to Watch'
    my_rating = request.form.get('rating')
    first_watch_date = request.form.get('firstwatchdate') or request.form.get('watchdate')
    watch_date = request.form.get('lastwatchdate') or request.form.get('watchdate')
    watch_with = request.form.get('watchedwith')
    comments = request.form.get('comments')

    if not title:
        return '<p>Title is required</p>', 400

    imdb_rating, genres, directors, stars, studios, length = None, None, None, None, None, None
    if imdb_fetch:
        omdb_movie_datum = query_omdb_api(title, year=aired, media_type=media_type)
        if omdb_movie_datum.get('Response', 'False') == 'False':
            return '<p>Movie not found on IMDB', 400
        aired = omdb_movie_datum['Year'] if 'Year' in omdb_movie_datum else None
        try:
            imdb_rating = [
                rating['Value'].partition('/')[0]
                for rating in omdb_movie_datum['Ratings']
                if rating['Source'] == 'Internet Movie Database'
            ][0] if 'Ratings' in omdb_movie_datum else None
        except Exception:
            imdb_rating = None
        genres = omdb_movie_datum['Genre'] if 'Genre' in omdb_movie_datum else None
        directors = omdb_movie_datum['Director'] if 'Director' in omdb_movie_datum else None
        stars = omdb_movie_datum['Actors'] if 'Actors' in omdb_movie_datum else None
        studios = omdb_movie_datum['Production'] if 'Production' in omdb_movie_datum else None
        length = omdb_movie_datum['Runtime'].partition(' min')[0] if 'Runtime' in omdb_movie_datum else None

    if status not in ('Watched', 'Dropped', 'Plan to Watch', 'In Progress', 'Caught Up', 'Casual Watch', None):
        return '<p>Provided status is invalid</p>', 400

    if status is None:
        status = 'Plan to Watch'

    conn = sqlite3.connect('movie_data.db')
    cursor = conn.cursor()

    q = 'SELECT id FROM movie WHERE title = ?' if media_type == 'movie' else 'SELECT id FROM show WHERE title = ?'
    p = [title]
    if 'year' in request.form:
        q += ' AND releaseDate = ?' if media_type == 'movie' else ' AND airingDates = ?'
        p.append(aired)
    existing_ids = cursor.execute(q, p).fetchall()
    existing_id = existing_ids[0][0] if existing_ids else None
    if existing_id is not None:
        if not replace:
            return '<p>Show already exists and replacement is not allowed</p>', 400
        match media_type:
            case 'movie':
                cursor.execute(
                    'UPDATE movie SET title=?, status=?, releaseDate=?, criticsRating=?, myRating=?, watchDate=?, watchedWith=?, genres=?, director=?, stars=?, studio=?, comments=?, length=? WHERE id=?;',
                    (title, status, aired, imdb_rating, my_rating, watch_date, watch_with, genres, directors, stars, studios, comments, length, existing_id)
                )
            case 'show':
                cursor.execute(
                    'UPDATE show SET title=?, status=?, airingDates=?, criticsRating=?, myRating=?, firstWatchDate=?, lastWatchDate=?, watchedWith=?, genres=?, director=?, stars=?, studio=?, comments=?, runtime=? WHERE id=?;',
                    (title, status, aired, imdb_rating, my_rating, first_watch_date, watch_date, watch_with, genres, directors, stars, studios, comments, length, existing_id)
                )
            case _:
                return '<p>Media type not supported</p>', 400
    else:
        if not insert:
            return '<p>Show not present and insertion is not allowed</p>', 400
        match media_type:
            case 'movie':
                cursor.execute(
                    'INSERT INTO movie (title, status, releaseDate, criticsRating, myRating, watchDate, watchedWith, genres, director, stars, studio, comments, length) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);',
                    (title, status, aired, imdb_rating, my_rating, watch_date, watch_with, genres, directors, stars, studios, comments, length)
                )
            case 'show':
                cursor.execute(
                    'INSERT INTO show (title, status, airingDates, criticsRating, myRating, firstWatchDate, lastWatchDate, watchedWith, genres, director, stars, studio, comments, runtime) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);',
                    (title, status, aired, imdb_rating, my_rating, watch_date, watch_with, genres, directors, stars, studios, comments, length)
                )
            case _:
                return '<p>Media type not supported</p>', 400
    conn.commit()
    return '<p>Success</p>', 200


def backfill_from_omdb(media_type: MEDIA_TYPE, num: int = 10):
    conn = sqlite3.connect('movie_data.db')
    cursor = conn.cursor()
    match media_type:
        case 'movie':
            q = "SELECT title FROM movie WHERE releaseDate IS NULL OR criticsRating IS NULL OR genres IS NULL OR director IS NULL OR stars IS NULL OR length IS NULL order by random() limit " + str(num)
        case 'show':
            q = "SELECT title FROM show WHERE airingDates IS NULL OR criticsRating IS NULL OR genres IS NULL OR director IS NULL OR stars IS NULL OR runtime IS NULL order by random() limit " + str(num)
        case _:
            return '<p>Media type not supported</p>', 400
    media_list = [m[0] for m in cursor.execute(q).fetchall()]
    omdb_data = fetch_omdb_data(media_type, media_list)
    sqlite_data_to_update = []
    oops = []
    for title, omdb_datum in omdb_data.items():
        if omdb_datum['Response'] == 'False':
            oops.append(title)
            continue

        aired = omdb_datum['Year'] if 'Year' in omdb_datum else ''
        try:
            imdb_rating = [
                rating['Value'].partition('/')[0]
                for rating in omdb_datum['Ratings']
                if rating['Source'] == 'Internet Movie Database'
            ][0] if 'Ratings' in omdb_datum else ''
        except Exception:
            imdb_rating = ''
        genres = omdb_datum['Genre'] if 'Genre' in omdb_datum else None
        directors = omdb_datum['Director'] if 'Director' in omdb_datum else None
        stars = omdb_datum['Actors'] if 'Actors' in omdb_datum else None
        studios = omdb_datum['Production'] if 'Production' in omdb_datum else None
        length = omdb_datum['Runtime'].partition(' min')[0] if 'Runtime' in omdb_datum else None
        sqlite_data_to_update.append((aired, imdb_rating, genres, directors, stars, studios, length, title))
    match media_type:
        case 'movie':
            cursor.executemany(
                'UPDATE movie SET releaseDate=?, criticsRating=?, genres=?, director=?, stars=?, studio=?, length=? WHERE title=?',
                sqlite_data_to_update
            )
        case 'show':
            cursor.executemany(
                'UPDATE show SET airingDates=?, criticsRating=?, genres=?, director=?, stars=?, studio=?, runtime=? WHERE title=?',
                sqlite_data_to_update
            )
        case _:
            return '<p>Media type not supported</p>', 400
    print(f'Updated {len(sqlite_data_to_update)} rows for {media_type}s {", ".join(x[7] for x in sqlite_data_to_update)}')
    print(f'Not found in OMDB: {[o.encode("utf-8") if o else "" for o in oops]}')
    conn.commit()
    conn.close()


def fetch_gspread_media_data(media_type: MEDIA_TYPE):
    if media_type not in get_args(MEDIA_TYPE):
        raise AssertionError('Invalid media type')
    media_data_exists = os.path.exists(f'out/gsheets_{media_type}_latest.pkl')
    fetch_from_gspread = True

    if media_data_exists:
        print(f'Pickled {media_type} data found on disk. Fetch from disk? (Y/N)')
        fetch_from_gspread = input().lower() != 'y'

    if fetch_from_gspread:
        print(f'No pickled {media_type} data found, querying google sheets API...')
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(os.environ['GOOGLE_AUTH_KEY_FILE'], scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(os.environ['MOVIE_SPREADSHEET_KEY']).worksheet(f'{media_type.title()}s (Original)')
        gspread_media_data = sheet.get_all_records()
        with open(f'out/gsheets_{media_type}_latest.pkl', 'wb') as f:
            # noinspection PyTypeChecker
            pickle.dump(gspread_media_data, f)
    elif media_data_exists:
        with open(f'out/gsheets_{media_type}_latest.pkl', 'rb') as f:
            gspread_media_data = pickle.load(f)
    else:
        raise SystemExit(f'No {media_type} data found')
    return gspread_media_data


def export_gspread_media_data(media_type: MEDIA_TYPE, cursor):
    if media_type not in get_args(MEDIA_TYPE):
        raise AssertionError('Invalid media type')
    q = (
        'SELECT title, releaseDate, status, subStatus, favorite, myRating, criticsRating, watchDate, watchedWith, genres, director, stars, studio, comments, length FROM movie'
        if media_type == 'movie' else
       'SELECT title, airingDates, status, subStatus, lastWatchedSeason, lastWatchedEpisode, favorite, myRating, criticsRating, firstWatchDate, lastWatchDate, watchedWith, genres, director, stars, studio, comments, runtime FROM show'
    )
    media_data_sqlite = cursor.execute(q).fetchall()
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(os.environ['GOOGLE_AUTH_KEY_FILE'], scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(os.environ['MOVIE_SPREADSHEET_KEY']).worksheet(f'{media_type.title()}s')
    current_gspread_media_cells = sheet.get_all_values()
    updated_gspread_media_cells = [current_gspread_media_cells[0]]
    if media_type == 'movie':
        updated_gspread_media_cells.extend([[row[0], row[1], row[2], row[3], 'TRUE' if row[4] else 'FALSE', row[5], row[6], row[7], row[8], row[9], '', row[10], row[11], row[12], row[13]] for row in media_data_sqlite])
    else:
        updated_gspread_media_cells.extend([[row[0], row[1], row[2], row[3], row[4], [5], 'TRUE' if row[6] else 'FALSE', row[7], row[8], row[9], row[10], row[11], row[12], '', row[13], row[14], row[15], row[16]] for row in media_data_sqlite])
    sheet.update(updated_gspread_media_cells)


def init_db(cursor):
    print('Initializing the database...')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS movie (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title VARCHAR(255) NOT NULL,
        releaseDate INT DEFAULT NULL,
        status VARCHAR(50) DEFAULT 'Plan to Watch',
        subStatus VARCHAR(50) DEFAULT 'N/A',
        favorite BOOLEAN DEFAULT FALSE,
        myRating DECIMAL(3, 2) DEFAULT NULL,
        criticsRating DECIMAL(3, 2) DEFAULT NULL,
        watchDate DATE DEFAULT NULL,
        watchedWith VARCHAR(255) DEFAULT NULL,
        genres VARCHAR(255) DEFAULT NULL,
        director VARCHAR(255) DEFAULT NULL,
        stars VARCHAR(255) DEFAULT NULL,
        studio VARCHAR(255) DEFAULT NULL,
        comments TEXT DEFAULT NULL,
        runtime INT DEFAULT NULL
    );
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS show (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title VARCHAR(255) NOT NULL,
        airingDates VARCHAR(50) DEFAULT NULL,
        status VARCHAR(50) DEFAULT 'Plan to Watch',
        subStatus VARCHAR(50) DEFAULT 'N/A',
        lastWatchedEpisode INT DEFAULT NULL,
        lastWatchedSeason INT DEFAULT NULL,
        favorite BOOLEAN DEFAULT FALSE,
        myRating DECIMAL(3, 2) DEFAULT NULL,
        criticsRating DECIMAL(3, 2) DEFAULT NULL,
        firstWatchDate DATE DEFAULT NULL,
        lastWatchDate DATE DEFAULT NULL,
        watchedWith VARCHAR(255) DEFAULT NULL,
        genres VARCHAR(255) DEFAULT NULL,
        director VARCHAR(255) DEFAULT NULL,
        stars VARCHAR(255) DEFAULT NULL,
        studio VARCHAR(255) DEFAULT NULL,
        comments TEXT DEFAULT NULL,
        runtime INT DEFAULT NULL
    );
    ''')


def upsert_into_db(media_type: MEDIA_TYPE, cursor, gspread_media_data):
    if media_type not in get_args(MEDIA_TYPE):
        raise AssertionError('Invalid media type')
    print('Inserting data into the database...')
    i = 0
    chunk = gspread_media_data[i:i + SQL_INSERT_CHUNK_SIZE]
    while len(chunk) > 0:
        q = '''
            INSERT OR IGNORE INTO movie (title, releaseDate, status, subStatus, favorite, myRating, criticsRating, watchDate, watchedWith, genres, director, stars, studio, comments, length)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''' if media_type == 'movie' else '''
            INSERT OR IGNORE INTO show (title, airingDates, status, subStatus, lastWatchedSeason, lastWatchedEpisode, favorite, myRating, criticsRating, firstWatchDate, lastWatchDate, watchedWith, genres, director, stars, studio, comments, runtime)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        p = [
            (
                row['Title'], row['Release Date'], row['Status'], row['Substatus'], row['Favorite'] == 'TRUE',
                row['My Rating'], row['Critic Rating'], row['Watch Date'], row['Watch(ed) With'],
                (row['Genre'] + ',' + row['Subgenre']).strip(',').replace(', ', ','), row['Director'], row['Stars'],
                row['Studio'], row['Comments'], None
            ) if media_type == 'movie' else
            (
                row['Title'], row['Years Aired'], row['Status'], row['Substatus'], row['Last Season Watched'],
                row['Last Episode Watched'], row['Favorite'] == 'TRUE', row['My Rating'], row['Critic Rating'],
                row['First Watch Date'], row['Last Watch Date'], row['Watch(ed) With'], row['Genre'], row['Director'],
                row['Stars'], row['Studio'], row['Comments'], None
            )
            for row in chunk
            if row['Title'] != ''
            ]
        cursor.executemany(q, p)
        i += SQL_INSERT_CHUNK_SIZE
        chunk = gspread_media_data[i:i + SQL_INSERT_CHUNK_SIZE]


def query_omdb_api(media_title: str, year: Optional[int] = None, media_type: Optional[MEDIA_TYPE] = None):
    q = f'https://www.omdbapi.com/?t="{media_title}"&apikey={os.getenv("OMDB_API_KEY")}'
    if media_type == 'movie':
        q += '&type=movie'
    elif media_type == 'show':
        q += '&type=series'
    if year:
        q += f'&y={year}'
    response = requests.get(q)
    response.raise_for_status()
    return response.json()


def fetch_omdb_data(media_type: MEDIA_TYPE, media_list: list[str]):
    omdb_data = {}
    for media in media_list:
        try:
            omdb_data[media] = query_omdb_api(media, media_type=media_type)
        except Exception as e:
            print(f'Error for item {media}: {e}')
    return omdb_data


def main():
    """
    Main function ran when the script is ran from the command line.
    Allows for import and export of data from and to the gspread spreadsheet.
    :return: None
    """
    dotenv.load_dotenv()
    print('Movie Spreadsheet Updater Import/Export')
    print('(I)mport database from gspread')
    print('(E)xport database to gspread')
    print('E(x)it')
    inp = input('>').lower()
    print('')
    if inp == 'i':
        media_type: str | None = None
        while media_type not in get_args(MEDIA_TYPE):
            media_type = input('media type? >').lower()
            if media_type not in get_args(MEDIA_TYPE):
                print('Invalid media type')
        media_type: MEDIA_TYPE
        conn = sqlite3.connect('movie_data.db')
        cursor = conn.cursor()
        gspread_media_data = fetch_gspread_media_data(media_type)
        init_db(cursor)
        conn.commit()
        upsert_into_db(media_type, cursor, gspread_media_data)
        conn.commit()
        conn.close()
    elif inp == 'e':
        media_type: str | None = None
        while media_type not in get_args(MEDIA_TYPE):
            media_type = input('media type? >').lower()
            if media_type not in get_args(MEDIA_TYPE):
                print('Invalid media type')
        media_type: MEDIA_TYPE
        conn = sqlite3.connect('movie_data.db')
        cursor = conn.cursor()
        export_gspread_media_data(media_type, cursor)
        conn.commit()
        conn.close()
    elif inp == 'x':
        raise SystemExit('Goodbye')
    else:
        raise ValueError('Invalid input')

if __name__ == '__main__':
    raise SystemExit(main())
