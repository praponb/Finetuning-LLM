from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("discounts").alias("element"),
    )
)

result = (
    exploded
    .select("effective_from", "element")
    .orderBy(F.col("effective_from").asc_nulls_first())
)
