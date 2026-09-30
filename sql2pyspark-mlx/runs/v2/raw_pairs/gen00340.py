from pyspark.sql import functions as F

order_summary = (
    df_payments
    .groupBy("order_id")
    .agg(F.max("paid_at").alias("latest_paid_at"))
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .select(F.col("o.id"), F.col("o.shipping_fee"), F.col("t.latest_paid_at"))
    .orderBy(F.col("id").desc(), "latest_paid_at")
)
