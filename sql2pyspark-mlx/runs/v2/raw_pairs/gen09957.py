from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.month("order_date").alias("order_month"))
    .agg(F.max("shipping_fee").alias("max_shipping_fee"))
    .filter(F.col("max_shipping_fee") < 24)
)
