from pyspark.sql import functions as F

valid_web_events = (
    df_web_events
    .filter(F.col("event_type").isin(["purchase", "search"]) & F.col("event_type").isin(["purchase", "add_to_cart"]))
)

agg = (
    valid_web_events
    .groupBy("page", "event_type")
    .agg(
        F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart_count"),
    )
)

result = (
    agg
    .filter(F.col("add_to_cart_count") > 35)
    .select("page", "add_to_cart_count")
    .orderBy(F.col("page").desc(), F.col("add_to_cart_count").desc())
)
