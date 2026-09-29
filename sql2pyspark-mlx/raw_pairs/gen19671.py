from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_time") < "2025-10-18")
    .groupBy(
        F.when(F.col("duration_sec") >= 477, "high").when(F.col("duration_sec") >= 120, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("page") == "/product", 1).otherwise(0)).alias("product_count"),
        F.min("event_time").alias("min_event_time"),
        F.avg("duration_sec").alias("avg_duration_sec"),
    )
    .orderBy("duration_sec_bucket")
)
