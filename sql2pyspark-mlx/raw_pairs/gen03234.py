from pyspark.sql import functions as F

flattened = df_carts.select("id", "coupon_codes", F.explode_outer("attributes").alias("k", "v"))

result = (
    flattened
    .select("id", "coupon_codes", "k")
    .orderBy(F.col("id").desc())
)
