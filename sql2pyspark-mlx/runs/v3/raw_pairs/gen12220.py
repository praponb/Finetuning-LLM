from pyspark.sql import functions as F

base = df_orders.filter((F.year("order_date") >= 2024) & (F.col("shipping_fee") >= 14))

orders_stats = (
    base
    .groupBy("channel")
    .agg(F.count("*").alias("row_count"))
)

result = (
    orders_stats
    .orderBy(F.col("row_count").desc(), "channel")
    .limit(10)
)
