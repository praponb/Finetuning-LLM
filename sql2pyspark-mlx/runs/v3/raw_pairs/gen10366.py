from pyspark.sql import functions as F

valid_orders = df_orders.filter(F.col("amount") > 470)

status_summary = (
    valid_orders
    .groupBy("status")
    .agg(F.max("order_date").alias("latest_order_date"))
)

result = status_summary.orderBy("latest_order_date", F.col("status").desc())
