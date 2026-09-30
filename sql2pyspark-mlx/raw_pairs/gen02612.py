from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type")
    .pivot("page", ["/checkout", "/cart", "/home"])
    .agg(F.max("duration_sec").alias("total"), F.count(F.lit(1)).alias("item_count"))
    .select(
        "event_type",
        F.col("/checkout_total").alias("checkout_total"),
        F.col("/checkout_item_count").alias("checkout_item_count"),
        F.col("/cart_total").alias("cart_total"),
        F.col("/cart_item_count").alias("cart_item_count"),
        F.col("/home_total").alias("home_total"),
        F.col("/home_item_count").alias("home_item_count"),
    )
)
