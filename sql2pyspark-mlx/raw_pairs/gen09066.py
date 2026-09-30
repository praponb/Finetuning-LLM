from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.to_date("event_time").alias("event_day"),
        "page",
        F.when(F.col("duration_sec") >= 534, "high").when(F.col("duration_sec") >= 130, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(
        F.count(F.when(F.col("event_type") == "page_view", 1)).alias("page_view_count"),
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
    )
    .filter(F.col("stddev_duration_sec") >= 50)
)
