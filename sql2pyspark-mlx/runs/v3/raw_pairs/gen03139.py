from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id") == 24)
    .groupBy("event_type")
    .agg(
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.count("duration_sec").alias("duration_sec_count"),
        F.min("duration_sec").alias("min_duration_sec"),
    )
    .filter(F.col("min_duration_sec") > 130)
    .drop("min_duration_sec")
)
