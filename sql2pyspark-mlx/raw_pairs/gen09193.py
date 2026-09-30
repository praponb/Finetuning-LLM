from pyspark.sql import functions as F

unpacked = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "tier_limits",
        "discounts",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    unpacked
    .groupBy(
        F.when(F.col("entry_price") >= 26, "LARGE").otherwise("SMALL").alias("entry_price_tier"),
        "plan",
    )
    .agg(
        F.percentile_approx("entry_price", [0.25, 0.5, 0.75]).alias("entry_price_quartiles"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.percentile_approx("entry_price", 0.9).alias("entry_price_p90"),
        F.round(F.avg("entry_price"), 2).alias("avg_entry_price"),
        F.max(F.when(F.col("plan") == "starter", F.col("entry_price"))).alias("starter_max_entry_price"),
        F.percentile_approx("entry_price", 0.5).alias("entry_price_p50"),
    )
    .orderBy("plan")
)
