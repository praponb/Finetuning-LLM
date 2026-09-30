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
    .select("id", "effective_from", "currency")
    .orderBy(F.col("effective_from").asc_nulls_first())
)
