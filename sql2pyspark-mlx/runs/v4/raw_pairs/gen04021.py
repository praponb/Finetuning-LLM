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
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    unpacked
    .groupBy(F.date_trunc("WEEK", "effective_from").alias("week_start"), "currency")
    .agg(
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("plan") == "enterprise", F.col("avg_tier_price")).otherwise(0)).alias("enterprise_avg_tier_price"),
    )
)
