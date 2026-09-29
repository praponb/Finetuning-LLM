from pyspark.sql import functions as F

result = df_web_events.select("session_id", "page", "event_time")
