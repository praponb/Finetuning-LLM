from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.date_format("event_time", "yyyy-MM").alias("event_month"))
    .agg(
        F.sum(F.when(F.col("event_type") == "search", 1).otherwise(0)).alias("search_count"),
        F.round(((100 * F.sum(F.when(F.col("event_type") == "click", 1).otherwise(0))) / F.count("*")), 1).alias("pct_click"),
        F.countDistinct("duration_sec").alias("unique_event_types"),
        F.count(F.when(F.col("page") == "/cart", 1)).alias("cart_count"),
    )
    .filter(F.col("cart_count") >= 4)
    .drop("cart_count")
)
