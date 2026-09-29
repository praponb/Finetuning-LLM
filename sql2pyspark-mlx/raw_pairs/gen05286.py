from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    parsed
    .withColumn("avg_tier_price_tier", F.when(F.col("avg_tier_price") >= 34, "PREMIUM").otherwise("STANDARD"))
    .rollup("plan", "avg_tier_price_tier")
    .agg(
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.round(F.avg("lowest_price"), 2).alias("avg_lowest_price"),
        F.sum(F.when(F.col("plan") == "starter", F.col("lowest_price")).otherwise(0)).alias("starter_lowest_price"),
        F.countDistinct("plan").alias("distinct_plans"),
        F.sum(F.when(F.col("plan") == "enterprise", F.col("avg_tier_price")).otherwise(0)).alias("enterprise_avg_tier_price"),
        F.count_if(F.col("currency") == "EUR").alias("eur_events"),
        F.grouping_id().alias("grouping_id"),
    )
)
