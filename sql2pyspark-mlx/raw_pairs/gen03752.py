from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id") == 35)
    .select("id", "event_time")
)
