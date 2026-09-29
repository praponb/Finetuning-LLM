from pyspark.sql import functions as F

payments_by_order = (
    df_payments
    .filter(F.col("method").isin(["promptpay", "cash", "paypal"]))
    .groupBy("order_id")
    .agg(
        F.max("amount").alias("max_amount"),
        F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias("failed_count"),
    )
)

result = (
    df_orders.alias("o")
    .join(payments_by_order.alias("pbo"), F.col("o.id") == F.col("pbo.order_id"), "left")
    .filter(F.col("pbo.failed_count") >= 52)
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("o.discount"),
        F.col("pbo.max_amount"),
        F.col("pbo.failed_count"),
    )
    .orderBy("failed_count")
)
