from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page", "event_type")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0))) / F.count("*")), 1).alias("pct_purchase"),
        F.count(F.when(F.col("event_type") == "search", 1)).alias("search_count"),
    )
    .filter(F.col("search_count") > 6)
    .select("page", "event_type", "pct_purchase", "search_count")
)
