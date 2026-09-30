from pyspark.sql import functions as F

valid_web_events = df_web_events.filter(F.col("event_time") < "2025-01-18")

web_events_stats = (
    valid_web_events
    .groupBy("page")
    .agg(
        F.count(F.when(F.col("page") == "/product", 1)).alias("product_count"),
        F.min("duration_sec").alias("min_duration_sec"),
    )
)

result = web_events_stats.orderBy(F.col("product_count").desc(), "page")
