from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .filter(F.col("currency").isin(["USD", "EUR", "THB"]))
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "discounts",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("avg_tier_price") >= 27, "HIGH").otherwise("LOW").alias("avg_tier_price_tier"),
    )
    .agg(
        F.round(F.avg("avg_tier_price"), 2).alias("avg_avg_tier_price"),
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.percentile_approx("avg_tier_price", 0.9).alias("avg_tier_price_p90"),
        F.count(F.when(F.col("plan") == "starter", 1)).alias("starter_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.size(F.collect_set("currency")).alias("num_currencies"),
    )
    .orderBy(F.col("avg_tier_price_p90").desc())
)
