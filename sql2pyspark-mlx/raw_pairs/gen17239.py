from pyspark.sql import functions as F

unpacked = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "currency",
        "tier_limits",
        "tier_prices",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    unpacked
    .groupBy(
        F.when(F.col("lowest_price") >= 36, "HIGH").otherwise("LOW").alias("lowest_price_tier"),
    )
    .agg(
        F.size(F.collect_set("currency")).alias("num_currencies"),
        F.sum(F.when(F.col("plan") == "enterprise", F.col("lowest_price")).otherwise(0)).alias("enterprise_lowest_price"),
    )
)
