from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~(F.col("session_id").isin([3, 20, 21, 34]) | (F.col("duration_sec") <= 417)))
    .groupBy(F.hour("event_time").alias("event_hour"), "event_type")
    .agg(F.max("event_time").alias("latest_event_time"))
)
