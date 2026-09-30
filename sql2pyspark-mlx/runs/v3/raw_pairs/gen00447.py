from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("session_id") == 32) | (F.col("event_type") == "page_view"))
    .groupBy(F.date_format("event_time", "yyyy-MM").alias("event_month"), "page")
    .agg(
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        F.max("event_time").alias("latest_event_time"),
        F.avg("duration_sec").alias("avg_duration_sec"),
    )
    .filter(F.col("avg_duration_sec") >= 210)
    .drop("avg_duration_sec")
)
