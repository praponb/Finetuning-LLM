from pyspark.sql import functions as F

result = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        "tier_limits",
        "tier_prices",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)
