from pyspark.sql import functions as F

payments_by_order = (
    df_payments
    .filter(F.col("method").isin(["card", "bank_transfer"]))
    .groupBy("order_id")
    .agg(
        F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias("failed_count"),
    )
)

result = (
    df_orders.alias("o")
    .join(payments_by_order.alias("pbo"), F.col("o.id") == F.col("pbo.order_id"), "left")
    .select(
        F.col("o.id"),
        F.col("o.discount"),
        F.col("o.customer_id"),
        F.col("pbo.failed_count"),
    )
)
