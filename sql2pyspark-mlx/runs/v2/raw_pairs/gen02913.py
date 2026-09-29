from pyspark.sql import functions as F

valid_web_events = (
    df_web_events
    .filter(F.col("session_id").isin([1, 6, 9]) & F.col("session_id").isin([18, 23, 40]))
    .withColumn("is_add_to_cart", F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0))
)

web_events_stats = (
    valid_web_events
    .groupBy(F.to_date("event_time").alias("event_day"), "session_id")
    .agg(
        (F.max("is_add_to_cart") - F.min("is_add_to_cart")).alias("is_add_to_cart_range"),
        F.count("is_add_to_cart").alias("is_add_to_cart_count"),
    )
)

result = (
    web_events_stats
    .filter(F.col("is_add_to_cart_count") < 7)
    .select("event_day", "session_id", "is_add_to_cart_range", "is_add_to_cart_count")
    .orderBy("is_add_to_cart_count", "event_day", "session_id")
    .limit(5)
)
