from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type") == "click")
    .groupBy("event_type")
    .agg(
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.count("duration_sec").alias("duration_sec_count"),
        F.max("event_time").alias("last_event_time"),
    )
    .filter(F.col("duration_sec_count") < 3)
    .orderBy(F.col("avg_duration_sec").desc())
)
