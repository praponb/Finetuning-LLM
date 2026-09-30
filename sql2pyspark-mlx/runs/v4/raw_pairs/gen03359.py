from pyspark.sql import functions as F

result = (
    df_price_tiers
    .filter(F.col("currency") == "EUR")
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_prices",
        F.array_min("tier_prices").alias("lowest_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)
