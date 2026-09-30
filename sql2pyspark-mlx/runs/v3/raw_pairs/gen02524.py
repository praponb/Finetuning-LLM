from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([9, 20, 37]) & F.col("session_id").isin([7, 30]))
    .groupBy("page")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("event_type") == "search", 1).otherwise(0))) / F.count("*")), 1).alias("pct_search"),
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        F.count(F.when(F.col("event_type") == "add_to_cart", 1)).alias("add_to_cart_count"),
    )
    .orderBy(F.col("add_to_cart_count").desc(), "pct_search", "page")
    .limit(3)
)
