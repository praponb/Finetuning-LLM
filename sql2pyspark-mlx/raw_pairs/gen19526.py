from pyspark.sql import functions as F

order_totals = (
    df_payments
    .filter(F.col("method") == "promptpay")
    .groupBy("order_id")
    .agg(
        F.count("*").alias("payment_count"),
        F.max("paid_at").alias("latest_paid_at"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("t"), F.col("o.id") == F.col("t.order_id"), "left")
    .filter(F.col("t.payment_count") > 8)
    .select(
        F.col("o.id"),
        F.col("o.discount"),
        F.col("t.payment_count"),
        F.col("t.latest_paid_at"),
    )
)
