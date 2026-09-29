from pyspark.sql import functions as F

order_totals = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.count(F.when(F.col("method") == "cash", 1)).alias("cash_count"),
        F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_count"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .filter(F.col("t.cash_count") >= 5)
    .select(F.col("o.shipping_fee"), F.col("t.cash_count"), F.col("t.refunded_count"))
)
