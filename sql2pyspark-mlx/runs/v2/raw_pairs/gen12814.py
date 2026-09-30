from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("session_id") == 39) & (F.col("event_time") >= "2024-03-01"))
)
