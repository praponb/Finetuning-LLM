from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([12, 29, 32]) & (F.col("session_id").isin([6, 7, 9]) | F.col("event_type").isin(["purchase", "click"])))
    .select("id", "event_time", "duration_sec")
)
