from pyspark.sql import functions as F

order_stats = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.sum(F.when(F.col("status") == "REFUNDED", 1).otherwise(0)).alias("refunded_count"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .select(F.col("o.id"), F.col("o.store_id"), F.col("t.refunded_count"))
    .orderBy(F.col("store_id").desc(), "id")
    .limit(5)
)
