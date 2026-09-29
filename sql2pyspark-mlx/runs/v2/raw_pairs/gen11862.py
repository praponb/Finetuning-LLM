from pyspark.sql import functions as F

payments_by_order = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.count("*").alias("payment_count"),
        F.min("paid_at").alias("min_paid_at"),
    )
)

result = (
    df_orders.alias("o")
    .join(payments_by_order.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.amount"),
        F.col("t.payment_count"),
        F.col("t.min_paid_at"),
    )
)
