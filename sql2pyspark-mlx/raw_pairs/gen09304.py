from pyspark.sql import functions as F

order_stats = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.count(F.when(F.col("method") == "bank_transfer", 1)).alias("bank_transfer_count"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .filter(F.col("os.bank_transfer_count") > 5)
    .select(F.col("o.store_id"), F.col("os.bank_transfer_count"))
    .orderBy("store_id")
)
