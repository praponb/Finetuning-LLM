from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        "event_type",
        F.when(F.col("duration_sec") >= 442, "high").when(F.col("duration_sec") >= 187, "mid").otherwise("low").alias("duration_sec_bucket"),
        F.date_format("event_time", "yyyy-MM").alias("event_month"),
    )
    .agg(
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.count(F.when(F.col("event_type") == "page_view", 1)).alias("page_view_count"),
    )
    .orderBy("duration_sec_bucket")
)
