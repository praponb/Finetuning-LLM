from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.date_format("event_time", "yyyy-MM").alias("event_month"))
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.count("*").alias("total_count"),
        F.countDistinct("duration_sec").alias("unique_sessions"),
    )
    .orderBy(F.col("total_duration_sec").desc())
)
