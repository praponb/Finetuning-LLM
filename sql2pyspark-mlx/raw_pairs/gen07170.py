from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("discounts").alias("element"))
)

result = (
    exploded
    .select("plan", "effective_from", "element")
    .orderBy(F.col("element").desc_nulls_last())
)
