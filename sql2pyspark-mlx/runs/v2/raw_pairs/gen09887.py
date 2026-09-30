from pyspark.sql import functions as F

order_summary = (
    df_payments
    .filter(F.hour("paid_at").between(10, 17))
    .groupBy("order_id")
    .agg(F.min("paid_at").alias("first_paid_at"))
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .select(F.col("o.customer_id"), F.col("os.first_paid_at"))
    .orderBy(F.col("customer_id").desc(), "first_paid_at")
)
