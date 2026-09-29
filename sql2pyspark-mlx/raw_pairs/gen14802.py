from pyspark.sql import functions as F

result = (
    df_price_tiers
    .filter(F.col("effective_from").between("2023-09-21", "2024-09-01"))
    .select(
        "id",
        "effective_from",
        "currency",
        "tier_limits",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)
