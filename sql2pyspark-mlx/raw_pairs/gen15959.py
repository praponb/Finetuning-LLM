from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type").isin(["add_to_cart", "page_view", "click"]))
    .groupBy(F.hour("event_time").alias("event_hour"))
    .agg(
        F.sum(F.when(F.col("page") == "/cart", F.col("duration_sec")).otherwise(0)).alias("cart_duration_sec"),
        F.count("duration_sec").alias("duration_sec_count"),
        F.round(((100 * F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0))) / F.count("*")), 1).alias("pct_purchase"),
    )
)
