from pyspark.sql import functions as F

unpacked = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "tier_prices",
        F.array_min("tier_prices").alias("lowest_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    unpacked
    .groupBy(
        F.when(F.col("lowest_price") >= 32, "HIGH").otherwise("LOW").alias("lowest_price_tier"),
    )
    .agg(
        F.countDistinct("plan").alias("distinct_plans"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.round(F.sum("entry_price") / F.nullif(F.count(F.when(F.col("plan") == "basic", 1)), F.lit(0)), 2).alias("entry_price_per_basic"),
    )
    .filter(F.col("distinct_plans") > 1)
)
