from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_time") >= "2023-03-25")
    .select("session_id", "event_time")
)
