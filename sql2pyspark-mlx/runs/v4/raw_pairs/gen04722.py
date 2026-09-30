from pyspark.sql import functions as F

unpacked = (
    df_price_tiers
    .filter(F.col("plan").isin(["basic", "pro", "starter"]))
    .select(
        "id",
        "plan",
        "currency",
        "tier_prices",
        "discounts",
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    unpacked
    .cube(
        F.when(F.col("avg_tier_price") >= 27, "PREMIUM").otherwise("STANDARD").alias("avg_tier_price_tier"),
        "plan",
    )
    .agg(
        F.round(F.sum("avg_tier_price") / F.nullif(F.count(F.when(F.col("currency") == "EUR", 1)), F.lit(0)), 2).alias("avg_tier_price_per_eur"),
        F.countDistinct("plan").alias("distinct_plans"),
    )
    .filter(F.col("distinct_plans") > 1)
    .orderBy(F.col("avg_tier_price_per_eur").desc())
)
