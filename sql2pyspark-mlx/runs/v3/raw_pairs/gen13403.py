from pyspark.sql import functions as F

order_stats = (
    df_payments
    .groupBy("order_id")
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .filter(F.col("os.avg_amount") >= 34)
    .select(F.col("o.id"), F.col("o.store_id"), F.col("os.avg_amount"))
    .orderBy(F.col("avg_amount").desc(), F.col("store_id").desc())
)
