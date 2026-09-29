from pyspark.sql import functions as F

base = df_orders.filter(F.col("store_id") == 26)

orders_stats = (
    base
    .groupBy(F.quarter("order_date").alias("order_quarter"), "store_id")
    .agg(F.max("order_date").alias("last_order_date"))
)

result = orders_stats.orderBy("store_id", F.col("order_quarter").desc())
