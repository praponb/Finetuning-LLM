from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type") != "add_to_cart")
    .groupBy("event_type", "page")
    .agg(
        F.min("duration_sec").alias("min_duration_sec"),
        F.count(F.when(F.col("event_type") == "add_to_cart", 1)).alias("add_to_cart_count"),
        F.countDistinct("duration_sec").alias("unique_event_types"),
    )
    .filter(F.col("add_to_cart_count") < 3)
    .orderBy("page", "event_type")
    .limit(5)
)
