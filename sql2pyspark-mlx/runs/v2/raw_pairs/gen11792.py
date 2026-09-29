from pyspark.sql import functions as F

order_totals = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).alias("success_count"),
        F.min("paid_at").alias("min_paid_at"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "inner")
    .filter(F.col("ot.success_count") >= 58)
    .select(
        F.col("o.customer_id"),
        F.col("o.amount"),
        F.col("ot.success_count"),
        F.col("ot.min_paid_at"),
    )
    .orderBy("success_count", "min_paid_at")
)
