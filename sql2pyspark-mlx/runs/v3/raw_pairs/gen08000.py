from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("store_id")
    .agg(F.max("shipping_fee").alias("max_shipping_fee"))
    .orderBy(F.col("store_id").desc(), F.col("max_shipping_fee").desc())
)
