from google.cloud import bigquery


import pandas as pd
from stop_words import get_stop_words
import re
import base64

def extract_hashtag(text):
    hashtag = re.findall(r"#(\w+)", text)
    return " ".join(hashtag)


def cleaner_txt(text):
    text = ''.join(text).lower()    
    ''' 
    Function to clean:
     - Http links
     - @ mention
     - special caracter
     - RT
    '''
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)|(^rt)", " ", text).split()) 

def tokenization(text):
    return re.split('\W+', text)


def tweet_cleaner(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    #pubsub_message = base64.b64decode(event['data']).decode('utf-8')
  
    client = bigquery.Client()
    # Perform a query.
    sql = (
        'SELECT  '+
        'text, retweet_count, favorite_count, hashtags,  user_location, '+
        ' user_verified, user_followers_count, user_friends_count, '+ 
        ' rt_retweet_count, rt_favorite_count,  rt_text, '+
        ' CAST(created_at as DATE) as date ' +
        ' FROM capstonettw.tweet ' +
        ' WHERE DATE(created_at) = CURRENT_DATE()'
    #    ' WHERE DATE(created_at) = "2019-11-14"'
    )

    # Insert the query result into a dataframe
    df = pd.read_gbq(sql, dialect='standard')


    # Extract all the words that starts with #
    df["hashtags"] = df.text.apply(extract_hashtag)

    
    # Transform RT features

    df["rt_text"] = df.rt_text.replace("empty","")
    df["text"] = df.text + df.rt_text
    df["retweet_count"] = df["retweet_count"] + df["rt_retweet_count"]
    df["favorite_count"] = df["favorite_count"] + df["rt_favorite_count"]
 
    # Use regex to clean the text
    df["text"] = df.text.apply(cleaner_txt) 

    # Drop all unecessaries features
    df.drop(columns=[ "rt_retweet_count", "rt_favorite_count", "rt_text"], inplace=True)
    df.drop(df[df.text ==''].index, inplace=True)
    
    try:
        def remove_stop_word(word_list):
            filter_word =  [w for w in word_list if not w in stop_words]
           
            return  ' '.join(filter_word)

        stop_words = list(get_stop_words('en'))         
    except Exception as e:
        print(e)
    
    # Split the sentence into a array of words
    df['text'] = df['text'].apply(tokenization)

    # Remove Stop words
    df['text'] = df.text.apply(remove_stop_word)

    try:
        table_id = os.environ['TABLE_ID']
        project_id = os.environ['PROJECT_ID']
        df.to_gbq(destination_table=table_id, project_id=project_id, if_exists='append')
    except Exception as e:
        print(e)
    

