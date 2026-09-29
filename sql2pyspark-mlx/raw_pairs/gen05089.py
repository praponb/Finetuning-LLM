from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("discounts").alias("val"))
)

result = (
    flattened
    .select("id", "plan", "currency")
    .orderBy(F.col("plan").desc(), "id")
)
