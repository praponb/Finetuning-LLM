from pyspark.sql import functions as F

unpacked = (
    df_price_tiers
    .select(
        "id",
        "currency",
        "tier_prices",
        "discounts",
        F.array_min("tier_prices").alias("lowest_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    unpacked
    .groupBy("currency")
    .agg(
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.countDistinct("id").alias("distinct_ids"),
        F.size(F.collect_set("currency")).alias("num_currencies"),
        F.count_if(F.col("currency") == "USD").alias("usd_events"),
        F.countDistinct("currency").alias("distinct_currencies"),
        F.round(F.sum("entry_price") / F.nullif(F.count(F.when(F.col("currency") == "THB", 1)), F.lit(0)), 2).alias("entry_price_per_thb"),
    )
)
