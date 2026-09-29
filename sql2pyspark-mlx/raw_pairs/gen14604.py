from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_limits",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("entry_price") >= 28, "HIGH").otherwise("LOW").alias("entry_price_tier"),
    )
    .agg(
        F.count_if(F.col("plan") == "basic").alias("basic_events"),
        F.percentile_approx("avg_tier_price", [0.25, 0.5, 0.75]).alias("avg_tier_price_quartiles"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
    )
    .orderBy(F.col("entry_price_tier").desc())
)
