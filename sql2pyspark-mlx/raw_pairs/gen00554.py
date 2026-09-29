from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .filter(F.col("effective_from").between("2024-02-01", "2024-03-26"))
    .select(
        "id",
        "effective_from",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("lowest_price") >= 26, "LARGE").otherwise("SMALL").alias("lowest_price_tier"),
    )
    .agg(
        F.max("lowest_price").alias("max_lowest_price"),
        F.count("*").alias("event_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("lowest_price", 0.5).alias("lowest_price_p50"),
        F.percentile_approx("entry_price", 0.95).alias("entry_price_p95"),
    )
)
