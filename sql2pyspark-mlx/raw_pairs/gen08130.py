from pyspark.sql import functions as F

flattened = df_price_tiers.select("id", "plan", F.explode_outer("discounts").alias("val"))

result = (
    flattened
    .select("id", "plan", "val")
    .orderBy(F.col("val").desc(), "id")
)
