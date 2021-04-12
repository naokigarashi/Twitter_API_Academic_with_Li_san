import requests
import os
import json
import time
import pandas as pd

#Academic申請後に生成される"Bearer Token(BT)"
BT = '###############################'

search_url = "https://api.twitter.com/2/tweets/search/all"

#実際にツイート取得をする関数(下の3つの関数を呼び出しながら)
def main(keyword, start_time, end_time):

    count = 0
    flag = True
    TWEET_LIMIT = 1000000    
    cols = ['created_at', 'id', ' text']
    normalized_data_old = pd.DataFrame(index=[], columns=cols)
    
    query_params = make_parm(keyword, start_time, end_time)#ここで取得条件が返される
    
    while flag:
        if count >= TWEET_LIMIT:
            break
    
        headers = create_headers(BT)
        time.sleep(1)
        json_response = connect_to_endpoint(search_url, headers, query_params)
        normalized_data = pd.json_normalize(json_response['data'])
        normalized_data_new = pd.concat([normalized_data＿old, normalized_data])
        normalized_data＿old=normalized_data_new
        print("total:"+str(len(normalized_data＿old))+"tweets")
    

        result_count = json_response['meta']['result_count']
        if 'next_token' in json_response['meta']:
            next_token = json_response['meta']['next_token']
            query_params['next_token'] = next_token
            count += result_count
            time.sleep(3)  # rate limit = 1 request/1 sec
            json_response = connect_to_endpoint(search_url, headers, query_params)
            normalized_data = pd.json_normalize(json_response['data'])
            normalized_data_new = pd.concat([normalized_data＿old, normalized_data])
            normalized_data＿old=normalized_data_new
            print("total:"+str(len(normalized_data＿old))+"tweets")

        else:
            flag = False
            
    normalized_data_last=normalized_data_old
    normalized_data_last.to_csv('AcademicAPI_tweet.csv')#csvファイルに保存


#与えられたパラメータからツイート取得条件(query_params)を生成する関数
def make_parm(keyword, start_time, end_time):

    #検索条件(今回はキーワード、開始時刻、修了時刻のみ)
    query_params = {'query': keyword ,
                    'tweet.fields': 'created_at',
                    #'expansions': 'author_id',
                    'start_time': start_time,
                    'end_time': end_time,
                    #'user.fields': 'description', # profile information of author
                    'max_results':500,  #一回のqueryは５００で上限らしい
                    'next_token' : {} #次のページにいくためのparam？
                   }
    return query_params


def create_headers(BT):
    headers = {"Authorization": "Bearer {}".format(BT)}
    return headers


def connect_to_endpoint(url, headers, params):
    response = requests.request("GET", search_url, headers=headers, params=params)
    #print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()


#取得条件を手動で設定して実行
main(keyword="テニス", start_time="2021-02-15T00:00:00Z", end_time="2021-02-15T09:00:00Z")
