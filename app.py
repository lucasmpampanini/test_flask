from flask import Flask, jsonify, request
from boto3.dynamodb.conditions import Key, Attr
import requests
import boto3
import uuid
import redis


app = Flask(__name__)

@app.route('/', methods=['PUT', 'GET'])
def core():
	artista = request.args['artista']
	client = redis.Redis(host='localhost', port=6379, db=0)

	dynamodb = boto3.resource('dynamodb')
	table = dynamodb.Table('artist2')

	

	if request.method == 'PUT':
		cache = request.args['cache']
		responsedb = table.query(
		KeyConditionExpression=Key('nome').eq(artista))
		items = responsedb['Items']
		if cache:
			if client.exists(artista) == 1:
				client.delete(artista)
			elif not items:
				table.put_item(Item={"artist_id": str(uuid.uuid4()), "nome": artista })

			return "cache limpo, dynamodb atualizado"	



	elif request.method == 'GET':

		response = request_song_artist(artista)
		json = response.json()
		lista_musicas = []
		for hit in json['response']['hits']:
			if artista.lower() in hit['result']['primary_artist']['name'].lower():
				musica = hit['result']['title']
				nome = hit['result']['primary_artist']['name']
				lista_musicas.append({"musica": musica, "artista": nome})

		if client.exists(artista) == 1:
			
			return jsonify({"artista": artista, "pesquisa": lista_musicas})

		elif client.exists(artista) == 0:
			client.set(artista, artista)
			client.expire(artista, 604800)
			table.put_item(Item={"artist_id": str(uuid.uuid4()), "nome": artista })

			return jsonify({"artista": artista, "pesquisa": lista_musicas})
	

def request_song_artist(artist_name):
    base_url = 'https://api.genius.com'
    headers = {'Authorization': 'Bearer ' + 'NWAqbziAfJnsNhIj9SpVhtgRKw7PH6uCGHyGkDMTX-UH6ly0sxFgHFy0uuE2fq8H'}
    search_url = base_url + '/search'
    data = {'q': artist_name}
    response = requests.get(search_url, data=data, headers=headers)

    return response



def cria_tabela():
	dynamodb.create_table(
    TableName='artist2',
    KeySchema=[
        {
            'AttributeName': 'nome',
            'KeyType': 'HASH'
        },
        {
            'AttributeName': 'artist_id',
            'KeyType': 'RANGE'
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'artist_id',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'nome',
            'AttributeType': 'S'
        },
    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 5,
        'WriteCapacityUnits': 5
    }
)

if __name__=="__main__":
	app.run()