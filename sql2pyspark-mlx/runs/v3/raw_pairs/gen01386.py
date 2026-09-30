from pyspark.sql import functions as F

base = df_orders.filter(F.col("customer_id").isNull() & F.col("customer_id").isNull())

orders_stats = (
    base
    .groupBy("store_id")
    .agg(
        F.max("order_date").alias("last_order_date"),
        F.sum(F.when(F.col("status") == "COMPLETED", 1).otherwise(0)).alias("completed_count"),
    )
)

result = (
    orders_stats
    .filter(F.col("completed_count") >= 24)
    .select("store_id", "completed_count")
)
