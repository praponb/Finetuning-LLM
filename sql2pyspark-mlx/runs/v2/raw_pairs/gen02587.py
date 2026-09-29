from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([4, 9, 28]) & (F.col("duration_sec") <= 217))
    .select("event_time", "event_type", "duration_sec")
)
