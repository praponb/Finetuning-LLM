from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_time") >= "2025-07-01")
    .groupBy("event_type")
    .agg(
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.count(F.when(F.col("event_type") == "add_to_cart", 1)).alias("add_to_cart_count"),
        F.max("event_time").alias("latest_event_time"),
    )
    .orderBy(F.col("avg_duration_sec").desc())
)
