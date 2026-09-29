from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([7, 24, 27, 34]) & (F.to_date("event_time") < "2025-03-08"))
    .groupBy(F.hour("event_time").alias("event_hour"))
    .agg(
        F.min("event_time").alias("first_event_time"),
        F.count("duration_sec").alias("duration_sec_count"),
    )
    .filter(F.col("duration_sec_count") < 5)
)
