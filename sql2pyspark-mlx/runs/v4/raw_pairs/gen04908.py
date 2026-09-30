from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .filter(F.col("plan").isin(["starter", "basic", "pro"]))
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_limits",
        "tier_prices",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    parsed
    .withColumn("quarter_start", F.date_trunc("QUARTER", "effective_from"))
    .groupingSets(
        [["quarter_start", "plan"], ["quarter_start"]],
        "quarter_start",
        "plan",
    )
    .agg(
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_rows"),
        F.percentile_approx("avg_tier_price", [0.25, 0.5, 0.75]).alias("avg_tier_price_quartiles"),
        F.percentile_approx("avg_tier_price", 0.5).alias("avg_tier_price_p50"),
        F.percentile_approx("avg_tier_price", [0.25, 0.5, 0.75]).alias("avg_tier_price_quartiles_2"),
        F.count_if(F.col("plan") == "starter").alias("starter_events"),
    )
    .orderBy(F.col("avg_tier_price_p50").desc())
)
