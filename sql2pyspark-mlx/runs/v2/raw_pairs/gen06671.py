from pyspark.sql import functions as F

order_summary = (
    df_payments
    .groupBy("order_id")
    .agg(F.max("paid_at").alias("latest_paid_at"))
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.order_date"),
        F.col("o.amount"),
        F.col("os.latest_paid_at"),
    )
    .orderBy("id", "order_date")
    .limit(5)
)
