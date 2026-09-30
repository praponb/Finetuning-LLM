from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") <= 29)
    .groupBy("status", "channel")
    .agg(F.max("order_date").alias("last_order_date"))
    .orderBy("last_order_date")
)
