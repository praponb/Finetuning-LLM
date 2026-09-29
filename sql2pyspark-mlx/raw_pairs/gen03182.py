from pyspark.sql import functions as F

result = (
    df_orders
    .agg(F.sum("shipping_fee").alias("total_shipping_fee"))
    .orderBy(F.col("total_shipping_fee").desc())
)
