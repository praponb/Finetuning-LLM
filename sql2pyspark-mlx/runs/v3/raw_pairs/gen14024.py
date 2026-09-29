from pyspark.sql import functions as F

order_stats = (
    df_payments
    .filter(F.to_date("paid_at") < "2025-06-01")
    .groupBy("order_id")
    .agg(
        F.max("amount").alias("max_amount"),
        F.sum("amount").alias("total_amount"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .select(
        F.col("o.id"),
        F.col("o.status"),
        F.col("os.max_amount"),
        F.col("os.total_amount"),
    )
    .orderBy("id")
)
