from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "currency",
        "tier_limits",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    parsed
    .cube(
        F.when(F.col("avg_tier_price") >= 32, "PREMIUM").otherwise("STANDARD").alias("avg_tier_price_tier"),
    )
    .agg(
        F.countDistinct("plan").alias("distinct_plans"),
        F.percentile_approx("avg_tier_price", [0.25, 0.5, 0.75]).alias("avg_tier_price_quartiles"),
        F.count_if(F.col("plan") == "enterprise").alias("enterprise_events"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_rows"),
        F.count_if(F.col("currency") == "EUR").alias("eur_events"),
    )
    .orderBy(F.col("avg_tier_price_tier").asc_nulls_last())
)
