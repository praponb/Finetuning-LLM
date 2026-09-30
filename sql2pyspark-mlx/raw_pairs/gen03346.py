from pyspark.sql import functions as F

result = (
    df_price_tiers
    .filter(F.month("effective_from").isin([2, 7, 8]))
    .select(
        "id",
        "effective_from",
        "tier_limits",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)
