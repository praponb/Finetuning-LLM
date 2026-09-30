from pyspark.sql import functions as F

order_stats = (
    df_payments
    .groupBy("order_id")
    .agg(F.avg("amount").alias("avg_amount"))
)

result = (
    df_orders.alias("orders")
    .join(order_stats.alias("order_stats"), F.col("orders.id") == F.col("order_stats.order_id"), "inner")
    .filter(F.col("order_stats.avg_amount") > 900)
    .select(F.col("orders.amount"), F.col("order_stats.avg_amount"))
    .orderBy(F.col("amount").desc())
)
