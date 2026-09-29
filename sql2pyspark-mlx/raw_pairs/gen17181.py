from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("discounts").alias("element"),
    )
)

result = (
    exploded
    .select("id", "effective_from", "currency", "element")
    .orderBy("id")
)
