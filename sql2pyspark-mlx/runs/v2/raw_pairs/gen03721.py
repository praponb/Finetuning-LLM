from pyspark.sql import functions as F

recent_orders = (
    df_orders
    .filter(F.col("status").isin(["SHIPPED", "COMPLETED", "CANCELLED"]) & (F.col("shipping_fee") >= 33) & (F.col("status") != "PENDING"))
)

orders_stats = (
    recent_orders
    .groupBy("store_id")
    .agg(F.max("discount").alias("max_discount"))
)

result = orders_stats.filter(F.col("max_discount") >= 0.21)
