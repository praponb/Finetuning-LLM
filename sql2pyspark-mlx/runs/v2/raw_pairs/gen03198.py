from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg(F.min("order_date").alias("min_order_date"))
)
