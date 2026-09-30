from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("o.id") == F.col("p.order_id"), "inner")
    .select(
        F.col("o.status"),
        F.col("o.amount"),
        F.col("p.status").alias("payment_status"),
        F.col("p.method"),
        F.col("p.paid_at"),
    )
    .orderBy("method", "payment_status")
)
