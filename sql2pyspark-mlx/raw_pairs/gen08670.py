from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("tier_value"),
    )
)

result = (
    lines
    .select("id", "plan", "effective_from", "tier_value")
    .orderBy(F.col("tier_value").asc_nulls_last())
)
