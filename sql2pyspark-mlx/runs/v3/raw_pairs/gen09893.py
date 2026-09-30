from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg(F.max("shipping_fee").alias("max_shipping_fee"))
    .filter(F.col("max_shipping_fee") < 24)
)
