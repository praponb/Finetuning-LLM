from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee").isNotNull() & (F.col("store_id") == 6))
    .groupBy("channel")
    .agg(F.min("order_date").alias("min_order_date"))
)
