from pyspark.sql import functions as F

payments_by_order = (
    df_payments
    .filter(F.col("paid_at") >= "2024-11-01")
    .groupBy("order_id")
    .agg(
        F.min("amount").alias("min_amount"),
        F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_count"),
    )
)

result = (
    df_orders.alias("o")
    .join(payments_by_order.alias("pbo"), F.col("o.id") == F.col("pbo.order_id"), "left")
    .filter(F.col("pbo.min_amount") >= 1250)
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("pbo.min_amount"),
        F.col("pbo.failed_count"),
    )
)
