from pyspark.sql import functions as F

exploded = (
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
    exploded
    .select("id", "plan", "effective_from", "currency", "tier_value")
    .orderBy("effective_from", F.col("currency").desc())
)
