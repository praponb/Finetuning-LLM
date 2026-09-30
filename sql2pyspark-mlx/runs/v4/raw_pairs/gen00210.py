from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("tier_value"),
    )
)

result = (
    exploded
    .select("plan", "effective_from", "currency", "tier_value")
    .orderBy("tier_value")
)
