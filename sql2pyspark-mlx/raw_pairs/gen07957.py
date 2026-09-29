from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_limits",
        "discounts",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    parsed
    .withColumn("week_start", F.date_trunc("WEEK", "effective_from"))
    .cube("week_start")
    .agg(
        F.percentile_approx("entry_price", [0.25, 0.5, 0.75]).alias("entry_price_quartiles"),
        F.percentile_approx("entry_price", [0.25, 0.5, 0.75]).alias("entry_price_quartiles_2"),
        F.round(F.sum("entry_price") / F.nullif(F.count(F.when(F.col("plan") == "starter", 1)), F.lit(0)), 2).alias("entry_price_per_starter"),
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.round(F.avg("lowest_price"), 2).alias("avg_lowest_price"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("entry_price_per_starter").desc())
)
