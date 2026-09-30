from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status").isin(["PENDING", "CANCELLED", "SHIPPED"]))
    .groupBy("channel", "customer_id")
    .agg(F.max("shipping_fee").alias("max_shipping_fee"))
)
