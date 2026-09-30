from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        "tier_limits",
        "tier_prices",
        "discounts",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    parsed
    .groupBy("currency")
    .agg(
        F.sum(F.when(F.col("currency") == "THB", F.col("avg_tier_price")).otherwise(0)).alias("thb_avg_tier_price"),
        F.round(F.avg("entry_price"), 2).alias("avg_entry_price"),
        F.round(F.sum("avg_tier_price") / F.nullif(F.count(F.when(F.col("currency") == "EUR", 1)), F.lit(0)), 2).alias("avg_tier_price_per_eur"),
        F.count("*").alias("event_count"),
        F.count("*").alias("event_count_2"),
        F.count_if(F.col("currency") == "THB").alias("thb_events"),
    )
    .orderBy(F.col("thb_avg_tier_price").asc_nulls_last())
)
