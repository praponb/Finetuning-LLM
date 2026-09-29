from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "tier_limits",
        "tier_prices",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("entry_price") >= 29, "LARGE").otherwise("SMALL").alias("entry_price_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.percentile_approx("entry_price", [0.25, 0.5, 0.75]).alias("entry_price_quartiles"),
        F.round(F.avg("entry_price"), 2).alias("avg_entry_price"),
    )
)
