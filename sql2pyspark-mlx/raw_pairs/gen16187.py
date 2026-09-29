from pyspark.sql import functions as F

result = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        "tier_limits",
        "discounts",
        F.array_min("tier_prices").alias("lowest_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)
