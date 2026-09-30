from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([26, 29, 30]) & (F.col("duration_sec") > 234))
    .select("id", "event_time", "duration_sec")
)
