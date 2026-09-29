from pyspark.sql import functions as F

result = df_web_events.select("duration_sec", "session_id", "event_type")
