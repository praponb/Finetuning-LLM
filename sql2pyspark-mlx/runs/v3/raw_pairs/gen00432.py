from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.to_date("event_time") < "2025-03-01")
    .groupBy("event_type", "page")
    .agg(
        F.count("*").alias("web_event_count"),
        F.max("duration_sec").alias("max_duration_sec"),
        F.avg("duration_sec").alias("avg_duration_sec"),
    )
    .orderBy(F.col("max_duration_sec").desc(), "event_type", "page")
    .limit(1)
)
