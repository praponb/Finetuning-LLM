from pyspark.sql import functions as F

order_summary = (
    df_payments
    .filter(F.col("paid_at") >= "2024-02-18")
    .groupBy("order_id")
    .agg(
        F.max("amount").alias("max_amount"),
        F.count(F.when(F.col("method") == "cash", 1)).alias("cash_count"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("O"), F.col("o.id") == F.col("O.order_id"), "inner")
    .filter(F.col("O.max_amount") > 1020)
    .select(
        F.col("o.id"),
        F.col("o.shipping_fee"),
        F.col("O.max_amount"),
        F.col("O.cash_count"),
    )
)
