from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") >= 296)
    .agg(
        F.max("event_time").alias("latest_event_time"),
        F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart_count"),
    )
)
