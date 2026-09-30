from pyspark.sql import functions as F

flattened = df_carts.select("id", "customer_id", F.explode("coupon_codes").alias("value"))

result = (
    flattened
    .select("value")
    .orderBy(F.col("value").asc_nulls_last())
)
