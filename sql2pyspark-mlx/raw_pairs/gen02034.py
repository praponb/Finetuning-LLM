from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        "event_type",
        F.when(F.col("duration_sec") >= 464, "high").when(F.col("duration_sec") >= 155, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("page_view_count"),
    )
    .filter(F.col("page_view_count") >= 36)
)
