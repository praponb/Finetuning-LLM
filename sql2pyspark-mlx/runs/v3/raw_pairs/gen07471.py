from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(((F.col("event_time") < "2023-09-01") & F.col("session_id").isin([4, 7, 37, 38])) | (F.col("session_id") == 29))
    .select("id", "event_time", "session_id", "duration_sec")
)
