from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select("id", "effective_from", "currency", F.explode("tier_prices").alias("val"))
)

result = (
    flattened
    .select("effective_from")
    .orderBy(F.col("effective_from").asc_nulls_last())
)
