from pyspark.sql import functions as F

order_stats = (
    df_payments
    .filter(F.col("amount").between(1080, 1340))
    .groupBy("order_id")
    .agg(F.sum("amount").alias("total_amount"))
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("O"), F.col("o.id") == F.col("O.order_id"), "inner")
    .filter(F.col("O.total_amount") >= 7900)
    .select(F.col("o.customer_id"), F.col("O.total_amount"))
    .orderBy("total_amount", F.col("customer_id").desc())
)
