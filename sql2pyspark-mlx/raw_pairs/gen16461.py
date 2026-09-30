from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status", F.month("order_date").alias("order_month"))
    .agg(F.max("order_date").alias("last_order_date"))
)
