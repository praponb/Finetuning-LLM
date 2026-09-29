from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .filter(F.col("currency").isin(["USD", "EUR"]))
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_prices",
        "discounts",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    flattened_attrs
    .rollup("plan")
    .agg(
        F.sum(F.when(F.col("currency") == "EUR", F.col("lowest_price")).otherwise(0)).alias("eur_lowest_price"),
        F.percentile_approx("lowest_price", [0.25, 0.5, 0.75]).alias("lowest_price_quartiles"),
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_rows"),
    )
)
