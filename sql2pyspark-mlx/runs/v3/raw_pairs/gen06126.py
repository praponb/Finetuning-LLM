from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type") == "search")
    .groupBy(
        F.when(F.col("page").isin(["/search", "/checkout"]), "tier_1").otherwise("other").alias("page_group"),
    )
    .agg(
        F.count("*").alias("total_count"),
        F.round(((100 * F.sum(F.when(F.col("page") == "/search", 1).otherwise(0))) / F.count("*")), 1).alias("pct_search"),
    )
    .orderBy("pct_search")
)
