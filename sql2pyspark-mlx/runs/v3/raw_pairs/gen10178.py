from pyspark.sql import functions as F

order_stats = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.sum(F.when(F.col("method") == "cash", F.col("amount")).otherwise(0)).alias("cash_amount"),
        F.sum(F.when(F.col("status") == "REFUNDED", F.col("amount")).otherwise(0)).alias("refunded_amount"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .filter(F.col("os.refunded_amount") >= 51)
    .select(
        F.col("o.customer_id"),
        F.col("o.discount"),
        F.col("os.cash_amount"),
        F.col("os.refunded_amount"),
    )
    .orderBy(F.col("discount").desc(), F.col("cash_amount").desc())
)
