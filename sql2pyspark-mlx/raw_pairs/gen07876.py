from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode("tier_limits").alias("tier_value"),
    )
)

result = (
    lines
    .select("id", "plan", "effective_from", "tier_value")
    .orderBy(F.col("effective_from").desc_nulls_last())
)
