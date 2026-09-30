from pyspark.sql import functions as F

payments_by_order = (
    df_payments
    .filter(F.col("method").isin(["bank_transfer", "paypal"]))
    .groupBy("order_id")
    .agg(F.min("paid_at").alias("first_paid_at"))
)

result = (
    df_orders.alias("o")
    .join(payments_by_order.alias("pbo"), F.col("o.id") == F.col("pbo.order_id"), "left")
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("o.order_date"),
        F.col("pbo.first_paid_at"),
    )
)
