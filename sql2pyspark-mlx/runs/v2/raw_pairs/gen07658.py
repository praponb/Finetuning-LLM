from pyspark.sql import functions as F

payments_by_order = (
    df_payments
    .filter(F.col("status").isin(["REFUNDED", "FAILED"]))
    .groupBy("order_id")
    .agg(
        F.count("*").alias("total_count"),
        F.min("amount").alias("min_amount"),
    )
)

result = (
    df_orders.alias("o")
    .join(payments_by_order.alias("pbo"), F.col("o.id") == F.col("pbo.order_id"), "inner")
    .select(F.col("o.shipping_fee"), F.col("pbo.total_count"), F.col("pbo.min_amount"))
)
