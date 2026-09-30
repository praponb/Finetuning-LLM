from pyspark.sql import functions as F

result = (
    df_orders
    .rollup("status", "customer_id")
    .agg(F.min("order_date").alias("min_order_date"))
    .orderBy(F.col("status").desc(), "customer_id")
)
