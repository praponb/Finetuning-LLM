from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page")
    .agg(
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        F.count(F.when(F.col("event_type") == "add_to_cart", 1)).alias("add_to_cart_count"),
    )
    .filter(F.col("add_to_cart_count") >= 5)
)
