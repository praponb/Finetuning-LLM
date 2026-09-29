from pyspark.sql import functions as F

base = (
    df_orders
    .filter(F.col("customer_id").isNotNull() & ((F.col("store_id") == 31) | F.col("customer_id").isin([27, 28, 29])))
)

orders_stats = (
    base
    .groupBy("status")
    .agg(F.max("order_date").alias("latest_order_date"))
)

result = orders_stats.orderBy(F.col("latest_order_date").desc(), F.col("status").desc())
