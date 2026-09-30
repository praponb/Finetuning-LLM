from pyspark.sql import functions as F

src = df_orders.filter((F.col("channel") == "partner") & (F.col("amount") < 1450))

orders_stats = (
    src
    .groupBy("status")
    .agg(F.count("*").alias("order_count"))
)

result = (
    orders_stats
    .select("status", "order_count")
    .orderBy("status")
)
