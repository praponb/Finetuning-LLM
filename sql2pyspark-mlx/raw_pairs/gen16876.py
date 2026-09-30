from pyspark.sql import functions as F

unpacked = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_limits",
        "tier_prices",
        "discounts",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
    )
)

result = (
    unpacked
    .groupBy(
        F.date_trunc("QUARTER", "effective_from").alias("quarter_start"),
        F.when(F.col("lowest_price") >= 36, "LARGE").otherwise("SMALL").alias("lowest_price_tier"),
    )
    .agg(
        F.max("entry_price").alias("max_entry_price"),
        F.size(F.collect_set("currency")).alias("num_currencies"),
        F.sum(F.when(F.col("plan") == "starter", F.col("lowest_price")).otherwise(0)).alias("starter_lowest_price"),
        F.count("*").alias("num_rows"),
    )
    .orderBy(F.col("quarter_start").asc_nulls_last())
)
