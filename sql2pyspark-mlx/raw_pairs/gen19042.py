from pyspark.sql import functions as F

flattened = df_price_tiers.select("id", "effective_from", F.explode("discounts").alias("val"))

result = (
    flattened
    .select("id", "effective_from")
    .orderBy(F.col("effective_from").desc_nulls_first())
)
