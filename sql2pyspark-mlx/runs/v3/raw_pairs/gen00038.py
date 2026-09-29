from pyspark.sql import functions as F

order_stats = (
    df_payments
    .filter(F.col("amount") < 1200)
    .groupBy("order_id")
    .agg(
        F.sum(F.when(F.col("method") == "bank_transfer", F.col("amount")).otherwise(0)).alias("bank_transfer_amount"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .select(F.col("o.id"), F.col("o.customer_id"), F.col("os.bank_transfer_amount"))
)
