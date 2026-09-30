from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .select(
        "id",
        "currency",
        "tier_limits",
        "tier_prices",
        "discounts",
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    parsed
    .cube(
        "currency",
        F.when(F.col("entry_price") >= 27, "HIGH").otherwise("LOW").alias("entry_price_tier"),
    )
    .agg(
        F.percentile_approx("entry_price", [0.25, 0.5, 0.75]).alias("entry_price_quartiles"),
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_count"),
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_rows"),
        F.max(F.when(F.col("currency") == "USD", F.col("entry_price"))).alias("usd_max_entry_price"),
        F.round(F.sum("entry_price") / F.nullif(F.count(F.when(F.col("currency") == "USD", 1)), F.lit(0)), 2).alias("entry_price_per_usd"),
    )
    .orderBy(F.col("entry_price_per_usd").asc_nulls_last())
)
