import csv
from pymongo import MongoClient
from slugify import slugify
import nltk
import sys
import requests
import json


def load_stop_words():
    stop_words = []
    with open('data/stop-words') as f:
        reader = csv.reader(f)
        for w in reader:
            stop_words.append(w[0])
        return stop_words

def load_movies_title(limit, offset):
    client = MongoClient('mongodb://localhost:27017/')
    db = client['torrents']
    collection = db[sys.argv[1]]
    torrents = []
    for torrent in collection.find({'category':'films'}).limit(limit).skip(offset):
        dict_torrent = dict(slug=torrent['slug'], title=torrent['title'])
        torrents.append(dict_torrent)
    return sorted(torrents)


def slugify_tokens(tokens):
    return [slugify(token) for token in tokens]

def token_action(tokens, stop_words):
    i = 0
    for token in tokens:
        try:
            if stop_words.index(token) is not None:
                return i
        except:
            i += 1
    return i

def nb_torrents():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['torrents']
    collection = db[sys.argv[1]]
    return collection.find({'category':'films'}).count()

def insert_info(infos):
    client = MongoClient('mongodb://localhost:27017/')
    db = client['torrents']
    collection = db['torrents_info']
    collection.insert(infos)

def retrieve_description(title):
    search_result = requests.get('http://api.themoviedb.org/3/search/movie?api_key=5192eb6331a3db50b6b388ae8941edc6&query='+title).json()
    if search_result['total_results'] == 1:
        detail_result = requests.get('http://api.themoviedb.org/3/movie/' + str(search_result['results'][0]['id']) + '?api_key=5192eb6331a3db50b6b388ae8941edc6')
        return json.loads(detail_result.text)['overview']
    return ''


def process_titles():
    nb = nb_torrents()
    i = 0
    limit = 100
    while i < nb:
        infos = []
        torrents = load_movies_title(limit, i)
        for torrent in torrents:
            tokens = nltk.word_tokenize(torrent['title'])
            index_stop_words = token_action(slugify_tokens(tokens), stop_words)
            title = ' '.join(tokens[:index_stop_words])
            infos.append(dict(slug=torrent['slug'], title=title, description=retrieve_description(title)))
        insert_info(infos)
        i += limit

stop_words = load_stop_words()
process_titles()
