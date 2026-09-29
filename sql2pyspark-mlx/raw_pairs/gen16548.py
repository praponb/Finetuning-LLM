from pyspark.sql import functions as F

payments_by_order = (
    df_payments
    .filter(F.col("order_id").isNull())
    .groupBy("order_id")
    .agg(F.max("amount").alias("max_amount"))
)

result = (
    df_orders.alias("o")
    .join(payments_by_order.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .select(F.col("o.shipping_fee"), F.col("o.channel"), F.col("t.max_amount"))
    .orderBy("max_amount", F.col("channel").desc())
)
