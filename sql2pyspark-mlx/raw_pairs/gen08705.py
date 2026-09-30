from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_limits",
        "discounts",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    flattened_attrs
    .rollup(
        "currency",
        F.date_trunc("QUARTER", "effective_from").alias("quarter_start"),
    )
    .agg(
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.count_if(F.col("plan") == "starter").alias("starter_events"),
        F.count(F.when(F.col("currency") == "THB", 1)).alias("thb_rows"),
    )
    .filter(F.col("thb_rows") > 1)
    .orderBy(F.col("currency").desc())
)
