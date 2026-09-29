from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("element"),
    )
)

result = (
    flattened
    .select("id", "currency", "element")
    .orderBy(F.col("id").desc_nulls_last())
)
