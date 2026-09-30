from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .filter(F.col("plan") != "pro")
    .select(
        "id",
        "plan",
        "currency",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("lowest_price") >= 34, "PREMIUM").otherwise("STANDARD").alias("lowest_price_tier"),
    )
    .agg(
        F.sum(F.when(F.col("currency") == "THB", F.col("avg_tier_price")).otherwise(0)).alias("thb_avg_tier_price"),
        F.avg("entry_price").alias("avg_entry_price"),
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_rows"),
        F.round(F.sum("entry_price") / F.nullif(F.count(F.when(F.col("plan") == "starter", 1)), F.lit(0)), 2).alias("entry_price_per_starter"),
    )
    .filter(F.col("thb_avg_tier_price") < 1000000)
    .orderBy(F.col("lowest_price_tier").desc_nulls_last())
)
