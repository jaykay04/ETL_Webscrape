import pandas as pd
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import os
import psycopg2


url = "https://dev.to/latest"
ua = UserAgent()
userAgent = ua.random
headers = {"user-agent": userAgent}
page = requests.get(url, headers = headers)

soup = BeautifulSoup(page.content, "html.parser")


blog_box = soup.find_all("div", class_ = "crayons-story__body")

links = []
titles = []
time_uploaded = []
authors = []
tags = []
reading_times = []

for box in blog_box:
    #links
    if box.find("h2", class_ = "crayons-story__title") is not None:
        link = box.find("h2", class_ = "crayons-story__title").a
        link = link["href"]
        links.append(link.strip())
    else:
        links.append("None")

    #titles
    if box.find("h2", class_ = "crayons-story__title") is not None:
        title = box.find("h2", class_ = "crayons-story__title")
        titles.append(title.text.replace("\n", "").strip())
    else:
        titles.append("None")

    #time_uploaded
    if box.find("time", attrs = {"datetime": True}) is not None:
        time_upload = box.find("time", attrs = {"datetime": True})
        time_upload = time_upload["datetime"]
        time_uploaded.append(time_upload)
    else:
        time_uploaded.append("None")

    #authors
    if box.find("a", class_ = "crayons-story__secondary fw-medium m:hidden") is not None:
        author = box.find("a", class_ = "crayons-story__secondary fw-medium m:hidden")
        authors.append(author.text.replace("\n", "").strip())
    else:
        authors.append("None")

    #tags
    if box.find("div", class_ = "crayons-story__tags") is not None:
        tag = box.find("div", class_ = "crayons-story__tags")
        tags.append(tag.text.replace("\n", " ").strip())
    else:
        tags.append("None")

    #reading_times
    if box.find("div",class_ = "crayons-story__save") is not None:
        reading_time = box.find("div",class_ = "crayons-story__save")
        reading_times.append(reading_time.text.replace("\n", "").strip())
    else:
        reading_times.append("None")


blog_df = pd.DataFrame(
    {
        "Link": links,
        "Title": titles,
        "Time_Uploaded": time_uploaded,
        "Author": authors,
        "Tag": tags,
        "Reading_Time": reading_times
    }
)

blog_df = blog_df[blog_df["Link"] != "None"]

blog_df.Link.to_list()

article = []
article_link = []

def get_full_content(url2):
    ua = UserAgent()
    userAgent = ua.random
    headers = {"user-agent": userAgent}
    page = requests.get(url2, headers = headers)

    soup2 = BeautifulSoup(page.content, "html.parser")
    #print(url2)



    content = soup2.find("div", class_ = "crayons-article__main")

    paragraphs = content.find_all("p")

    contents = []

    for x in paragraphs:
        contents.append(x.text.replace("\n", " "))

    full_content = " ".join(contents)
    article.append(full_content)
    article_link.append(url2)

for i in blog_df.Link:
    get_full_content(i)

article_df = pd.DataFrame(
    {
        "Link": article_link,
        "Article_Content": article
    }
)

merged_df = blog_df.merge(article_df, on = "Link", how = "inner")


from nltk.corpus import stopwords
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

# Download the stopwords dataset
nltk.download("stopwords")
nltk.download("punkt")
nltk.download("wordnet")
nltk.download("vader_lexicon")
nltk.download("punkt_tab")


def count_words_without_stopwords(text):
    if isinstance(text, (str, bytes)):
        words = nltk.word_tokenize(str(text))
        stop_words = set(stopwords.words('english'))
        filtered_words = [word for word in words if word.lower() not in stop_words]
        return len(filtered_words)
    else:
        0

merged_df['Word_Count'] = merged_df["Article_Content"].apply(count_words_without_stopwords)


sent = SentimentIntensityAnalyzer()

def get_sentiment(record):
    sentiment_scores = sent.polarity_scores(record)
    compound_score = sentiment_scores['compound']

    if compound_score >= 0.05:
        sentiment = 'Positive'
    elif compound_score <= -0.05:
        sentiment = 'Negative'
    else:
        sentiment = 'Neutral'

    return compound_score, sentiment

merged_df[['Compound_Score' ,'Sentiment']] = merged_df['Article_Content'].astype(str).apply(lambda x: pd.Series(get_sentiment(x)))

import langid
import pycountry

def detect_language(text):
    # Convert Nan to an empty string
    text = str(text) if pd.notna(text) else ''

    # Use langid to detect the language
    lang, confidence = langid.classify(text)
    return lang

merged_df['Language'] = merged_df['Article_Content'].apply(detect_language)
merged_df['Language'] = merged_df['Language'].map(lambda code: pycountry.languages.get(alpha_2 = code).name if pycountry.languages.get(alpha_2 = code) else code)


filtered_df = merged_df[merged_df['Language'] == 'English'].reset_index(drop = True)
filtered_df['Reading_Time'] = filtered_df['Reading_Time'].str.replace(' min read', '', regex=False).str.strip().astype(int)
filtered_df.head()


# CREATE TABLE IF NOT EXISTS articles(
# Link TEXT,
# Title TEXT,
# Time_Uploaded TIMESTAMP,
# Author TEXT,
# Tag TEXT,
# Reading_Time INTEGER,
# Article_Content TEXT,
# Word_Count INTEGER,
# Compound_Score NUMERIC,
# Sentiment TEXT,
# Language TEXT
# );

db_params = {
    "dbname": "postgres",
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": "5432"
}

try:
    # Connect to PostgreSQL
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # SQL Insert Query
    insert_query = """
    INSERT INTO articles (Link, Title, Time_Uploaded, Author, Tag, Reading_Time, Article_Content, Word_Count, Compound_Score, Sentiment, Language)
    VALUES (%s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s)
    ON CONFLICT (Link) DO NOTHING;  -- Avoids duplicate primary key errors
    """

    # Insert DataFrame records one by one
    for _, row in filtered_df.iterrows():
        cursor.execute(insert_query, (
            row['Link'], row['Title'], row['Time_Uploaded'],  row['Author'], row['Tag'], row['Reading_Time'],
            row['Article_Content'],row['Word_Count'],row['Compound_Score'],row['Sentiment'],row['Language']
        ))

    # Commit and close
    conn.commit()
    print("Data inserted successfully!")

except Exception as e:
    print(e)

finally:
    if conn:
        cursor.close()
        conn.close





