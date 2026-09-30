from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type")
    .agg(
        F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart_count"),
        F.max("event_time").alias("latest_event_time"),
    )
    .filter(F.col("add_to_cart_count") < 79)
    .select("event_type", "add_to_cart_count", "latest_event_time")
    .orderBy(F.col("add_to_cart_count").desc(), "event_type")
    .limit(10)
)
