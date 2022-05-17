import spotipy
import pandas as pd
import psycopg2
import os

from sqlalchemy import create_engine
from spotipy.oauth2 import SpotifyOAuth

# Variables ctrl+alt+shift+l

CLIENT_ID = os.getenv('SPOTIFY_CLIENT_ID')
CLIENT_SECRET_ID = os.getenv('SPOTIFY_CLIENT_SECRET')


def spotify_extract_info():
    spotify_redirect_url = "http://localhost:8888/callback"

    sp_connect = spotipy.Spotify(auth_manager=SpotifyOAuth(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET_ID,
        redirect_uri=spotify_redirect_url,
        scope="user-read-recently-played"
    ))

    data = sp_connect.current_user_recently_played(limit=50)


    if len(data) == 0:
        print("data not found")

    else:
        # Album
        # Creacion de la estructura de los datos para la entidad Album
        album_list = []
        for row in data['items']:
            album_id = row['track']['album']['id']
            album_name = row['track']['album']['name']
            album_url = row['track']['album']['external_urls']['spotify']
            album_release_date = row['track']['album']['release_date']
            album_total_tracks = row['track']['album']['total_tracks']

            album_element = {
                'album_id': album_id,
                'album_name': album_name,
                'album_url': album_url,
                'album_release_date': album_release_date,
                'album_total_tracks': album_total_tracks

            }

            album_list.append(album_element)


        # Artistas
        # Creacion de la estructura de datos para la entidad Artista
        artist_dict = {}
        id_list = []
        name_list = []
        url_list = []

        for item in data['items']:
            for key, value in item.items():
                if key == "track":
                    for point in value['artists']:
                        id_list.append(point['id'])
                        name_list.append(point['name'])
                        url_list.append(point['external_urls']['spotify'])

        artist_dict = {
            'artist_id': id_list,
            'artist_name': name_list,
            'url_list': url_list

        }

        # Canciones
        # Crear la estructura de los datos para los tracks
        # id, nombre, url, popularity, duration_ms, album_id, artist_id
        track_list = []
        for row in data['items']:
            track_id = row['track']['id']
            track_name = row['track']['name']
            track_url = row['track']['external_urls']['spotify']
            track_popularity = row['track']['popularity']
            track_duration_ms = row['track']['duration_ms']
            track_album_id = row['track']['album']['id']
            track_artist_id = row['track']['artists'][0]['id']
            track_played_at = row['played_at']

            track_element = {
                'track_id': track_id,
                'track_name': track_name,
                'track_url': track_url,
                'track_popularity': track_popularity,
                'track_duration_ms': track_duration_ms,
                'track_album_id': track_album_id,
            }

        track_list.append(track_element)


        # Despues  de crear las estructuras de datos de cada entidad, vamos a cargar los datos en
        # dataframes para manejar la data de una forma adecuada y facil.

        # Cargar los datos desde la entidad album a un dataframe
        album_df = pd.DataFrame.from_dict(album_list)

        # Cargar los datos ...
        artist_df = pd.DataFrame.from_dict(album_list)

        # Cargar los datos desde la entidad track a un dataFrame
        track_df = pd.DataFrame.from_dict(artist_dict)

        # Borrar duplicados de Album
        album_df = album_df.drop_duplicates(subset=['album_id'])

        # Borrar duplicados de Album
        artist_dict = album_df.drop_duplicates(subset=['artist_id'])

        # Borrar ...
        artist_df = artist_df.drop_duplicates(['artist_id'])

        #print(track_df.transpose())

        print("antes de Que pase")
        print(track_df["track_played_at"])

        track_df['track_played_at'] = pd.to_datetime(track_df["track_played_at"],format="%Y-%m-%d %H:%M:%S")
        track_df['local_time_played_at'] = track_df["track_played_at"].dt.tz_convert(tz='America/Bogota')
        track_df['short_date_played_at'] = track_df["track_played_at"].astype(str).str[:10]
        print("despues de")
        print(track_df['short_date_played_at'])

#spotify_extract_info()
