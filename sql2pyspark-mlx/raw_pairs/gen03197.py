from pyspark.sql import functions as F

valid_web_events = (
    df_web_events
    .filter((F.col("duration_sec") == 317) & ((F.col("event_type") == "add_to_cart") | ~F.col("page").isin(["/cart", "/home"])))
)

agg = (
    valid_web_events
    .groupBy("event_type")
    .agg(
        F.sum(F.when(F.col("page") == "/cart", F.col("duration_sec")).otherwise(0)).alias("cart_duration_sec"),
    )
)

result = (
    agg
    .select("event_type", "cart_duration_sec")
    .orderBy(F.col("cart_duration_sec").desc(), "event_type")
    .limit(5)
)
