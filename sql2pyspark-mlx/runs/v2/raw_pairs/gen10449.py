from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg(F.avg("shipping_fee").alias("avg_shipping_fee"))
)
