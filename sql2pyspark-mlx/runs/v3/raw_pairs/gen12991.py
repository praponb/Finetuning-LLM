from pyspark.sql import functions as F

order_totals = (
    df_payments
    .groupBy("order_id")
    .agg(F.max("paid_at").alias("last_paid_at"))
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "inner")
    .select(F.col("o.store_id"), F.col("ot.last_paid_at"))
    .orderBy(F.col("store_id").desc())
)
