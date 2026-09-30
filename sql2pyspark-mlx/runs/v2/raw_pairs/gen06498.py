from pyspark.sql import functions as F

payments_by_order = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.min("paid_at").alias("min_paid_at"),
        F.count(F.when(F.col("method") == "card", 1)).alias("card_count"),
    )
)

result = (
    df_orders.alias("o")
    .join(payments_by_order.alias("pbo"), F.col("o.id") == F.col("pbo.order_id"), "left")
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("o.amount"),
        F.col("pbo.min_paid_at"),
        F.col("pbo.card_count"),
    )
    .orderBy(F.col("customer_id").desc())
)
