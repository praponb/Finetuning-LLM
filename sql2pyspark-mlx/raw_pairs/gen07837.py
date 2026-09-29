from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("discounts").alias("element"),
    )
)

result = (
    flattened
    .select("id", "effective_from", "element")
    .orderBy(F.col("effective_from").desc_nulls_first())
)
