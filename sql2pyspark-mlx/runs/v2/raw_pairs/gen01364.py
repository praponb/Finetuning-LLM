from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([4, 17, 30]))
    .select("event_time", "duration_sec", "page")
)
