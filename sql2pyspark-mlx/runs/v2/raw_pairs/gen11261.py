from pyspark.sql import functions as F

order_stats = (
    df_payments
    .groupBy("order_id")
    .agg(F.min("paid_at").alias("first_paid_at"))
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("t"), F.col("o.id") == F.col("t.order_id"), "left")
    .select(F.col("o.channel"), F.col("o.amount"), F.col("t.first_paid_at"))
    .orderBy(F.col("channel").desc(), F.col("first_paid_at").desc())
)
