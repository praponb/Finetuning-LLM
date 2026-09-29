from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id") == 35)
    .groupBy("status", "channel")
    .agg(F.min("order_date").alias("min_order_date"))
)
