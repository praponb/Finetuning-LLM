from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type", "page")
    .agg(
        F.min("duration_sec").alias("min_duration_sec"),
        F.min("event_time").alias("first_event_time"),
    )
    .filter(F.col("min_duration_sec") >= 390)
    .select("event_type", "page", "min_duration_sec", "first_event_time")
    .orderBy("first_event_time", "event_type", "page")
    .limit(1)
)
