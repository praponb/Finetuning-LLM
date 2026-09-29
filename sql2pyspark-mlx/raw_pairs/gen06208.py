from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_limits",
        "tier_prices",
        F.array_min("tier_prices").alias("lowest_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    parsed
    .withColumn("activity_month", F.date_trunc("MONTH", "effective_from"))
    .groupingSets(
        [["activity_month", "plan"], ["activity_month"]],
        "activity_month",
        "plan",
    )
    .agg(
        F.sum(F.when(F.col("plan") == "basic", F.col("entry_price")).otherwise(0)).alias("basic_entry_price"),
        F.percentile_approx("entry_price", [0.25, 0.5, 0.75]).alias("entry_price_quartiles"),
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.count("*").alias("cnt"),
    )
)
