from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_prices",
        F.array_min("tier_prices").alias("lowest_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    parsed
    .withColumn("lowest_price_tier", F.when(F.col("lowest_price") >= 27, "LARGE").otherwise("SMALL"))
    .rollup("lowest_price_tier")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count_if(F.col("plan") == "enterprise").alias("enterprise_events"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.count("*").alias("cnt"),
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("plan") == "starter", 1)).alias("starter_rows"),
        F.grouping_id().alias("grouping_id"),
    )
)
