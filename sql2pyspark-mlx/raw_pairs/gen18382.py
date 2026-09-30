from pyspark.sql import functions as F

unpacked = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "tier_prices",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
    )
)

result = (
    unpacked
    .groupBy("plan")
    .agg(
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.percentile_approx("entry_price", 0.5).alias("entry_price_p50"),
        F.round(F.sum("entry_price") / F.nullif(F.count(F.when(F.col("plan") == "basic", 1)), F.lit(0)), 2).alias("entry_price_per_basic"),
        F.round(F.avg("avg_tier_price"), 2).alias("avg_avg_tier_price"),
        F.sort_array(F.collect_set("plan")).alias("plan_list_2"),
        F.round(F.sum("entry_price") / F.nullif(F.count(F.when(F.col("plan") == "basic", 1)), F.lit(0)), 2).alias("entry_price_per_basic_2"),
    )
)
