from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("tier_value"),
    )
)

result = (
    exploded
    .select("id", "effective_from", "currency")
    .orderBy(F.col("effective_from").desc(), F.col("id").desc())
)
