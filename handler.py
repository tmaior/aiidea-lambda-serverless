import boto3
import os
import tweepy
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from dotenv import load_dotenv
import json

# load system env's
load_dotenv()

# Twitter API Credentials
API_KEY = os.environ.get("API_KEY")
API_SECRET_KEY = os.environ.get("API_SECRET_KEY")
ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN")
ACCESS_TOKEN_SECRET = os.environ.get("ACCESS_TOKEN_SECRET")

BEARER_TOKEN = os.environ.get("BEARER_TOKEN")
CONSUMER_SECRET = os.environ.get("CONSUMER_SECRET")
CONSUMER_KEY = os.environ.get("CONSUMER_KEY")

# AWS Credentials
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID_ENV")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY_ENV")

# S3 Bucket Details
BUCKET_NAME = os.environ.get("BUCKET_NAME")

# Initialize Tweepy Client
client = tweepy.Client(bearer_token=BEARER_TOKEN)

# Initialize Boto3 S3 Client
s3 = boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)


def fetch_tweets(query, count=100):
    # Fetch tweets using the Tweepy API.
    tweets = client.search_recent_tweets(query=query, tweet_fields=['author_id', 'lang', 'created_at'])
    return tweets.data


def store_data_in_s3(data, folder):


    # Store data in S3 in the required directory structure and format.
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    raw_data_key = f"raw_data/{folder}/{timestamp}.json"
    directory_path = f"processed_data/{folder}/"
    processed_data_key = f"{directory_path}{timestamp}.parquet"

    # create the file as parquet
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)

    # Convert data to parquet and store in AWS
    table = pa.Table.from_pandas(data)
    pq.write_table(table, processed_data_key)
    s3.upload_file(processed_data_key, BUCKET_NAME, processed_data_key)

    # Store raw data
    s3.put_object(Bucket=BUCKET_NAME, Key=raw_data_key, Body=json.dumps(data.to_json(orient="records"), indent=4))

    


def handler(event, context):
    try:
        # take the query params
        query_params = event.get('queryStringParameters', {})

        # take the param in the url
        query = query_params.get('query')
        tweets = fetch_tweets(query)



        data_list = []
        for tweet in tweets:
            data = {
                "ID": tweet.id,
                "Raw_Content": tweet.text
            }
            data_list.append(data)

        print("-----------------------------")
        print(data_list)

        # Convert data to DataFrame
        df = pd.DataFrame(data_list)

        # Store data in S3
        store_data_in_s3(df, 'tweets')
        return {
            'statusCode': 200,
            'body': json.dumps('Data uploaded with success!')
        }
    except Exception as error:
        return error
