from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("tier_value"),
    )
)

result = (
    flattened
    .select("id", "effective_from", "currency")
    .orderBy(F.col("effective_from").desc_nulls_first())
)
