from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") != 398)
    .agg(
        F.count(F.when(F.col("event_type") == "add_to_cart", 1)).alias("add_to_cart_count"),
    )
)
