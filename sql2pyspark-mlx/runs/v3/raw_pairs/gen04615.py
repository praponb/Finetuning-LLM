from pyspark.sql import functions as F

order_summary = (
    df_payments
    .filter(F.col("paid_at") < "2025-01-01")
    .groupBy("order_id")
    .agg(F.max("amount").alias("max_amount"))
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .filter(F.col("t.max_amount") > 900)
    .select(F.col("o.customer_id"), F.col("o.channel"), F.col("t.max_amount"))
    .orderBy("channel", F.col("customer_id").desc())
)
