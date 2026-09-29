from pyspark.sql import functions as F

result = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_limits",
        "tier_prices",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)
