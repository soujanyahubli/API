import requests
import time
import pandas as pd
from sqlalchemy import create_engine, Column, String, Float, Integer, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime
from collections import defaultdict


API_KEY = '76c3ebd7e7bd66dd46e2201753992b56'  
CITIES = ['Delhi', 'Mumbai', 'Chennai', 'Bangalore', 'Kolkata', 'Hyderabad']
INTERVAL = 300  
TEMP_UNITS = 'metric' 
THRESHOLD = 35  


Base = declarative_base()

class DailyWeatherSummary(Base):
    __tablename__ = 'daily_weather_summary'
    id = Column(Integer, primary_key=True)
    city = Column(String)
    avg_temp = Column(Float)
    max_temp = Column(Float)
    min_temp = Column(Float)
    dominant_condition = Column(String)
    date = Column(DateTime)

DATABASE_URL = "sqlite:///weather.db"
engine = create_engine(DATABASE_URL)
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

def get_weather_data(city):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units={TEMP_UNITS}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return {
            'city': city,
            'temp': data['main']['temp'],
            'feels_like': data['main']['feels_like'],
            'weather': data['weather'][0]['main'],
            'timestamp': data['dt']
        }
    print(f"Error fetching data for {city}: {response.status_code}")
    return None

def fetch_weather_for_cities():
    weather_data = []
    for city in CITIES:
        data = get_weather_data(city)
        if data:
            weather_data.append(data)
    return weather_data

def daily_summary(weather_data):
    df = pd.DataFrame(weather_data)
    daily_avg_temp = df['temp'].mean()
    daily_max_temp = df['temp'].max()
    daily_min_temp = df['temp'].min()
    dominant_condition = df['weather'].mode()[0]  

    summary = {
        'avg_temp': daily_avg_temp,
        'max_temp': daily_max_temp,
        'min_temp': daily_min_temp,
        'dominant_condition': dominant_condition
    }
    return summary

def save_daily_summary(summary, city):
    weather_entry = DailyWeatherSummary(
        city=city,
        avg_temp=summary['avg_temp'],
        max_temp=summary['max_temp'],
        min_temp=summary['min_temp'],
        dominant_condition=summary['dominant_condition'],
        date=datetime.now()
    )
    session.add(weather_entry)
    session.commit()

def check_temperature_alerts(weather_data):
    for data in weather_data:
        if data['temp'] > THRESHOLD:
            print(f"ALERT: {data['city']} temperature {data['temp']}°C exceeds {THRESHOLD}°C")

def process_data():
    daily_data = defaultdict(list)
    
    while True:
        weather_data = fetch_weather_for_cities()
        if weather_data:
            for data in weather_data:
                daily_data[data['city']].append(data)

            check_temperature_alerts(weather_data)
            
            if len(daily_data[CITIES[0]]) >= 2:  
                for city in CITIES:
                    summary = daily_summary(daily_data[city])
                    print(f"Daily Summary for {city}: {summary}")
                    save_daily_summary(summary, city)
                    daily_data[city] = []  
                
        time.sleep(INTERVAL)

if __name__ == '__main__':
    process_data()
