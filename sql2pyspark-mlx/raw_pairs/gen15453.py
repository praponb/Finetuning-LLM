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
    .select("effective_from", "currency", "tier_value")
    .orderBy(F.col("effective_from").asc_nulls_last())
)
