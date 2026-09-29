from pyspark.sql import functions as F

order_totals = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.min("paid_at").alias("first_paid_at"),
    )
)

result = (
    df_orders.alias("orders")
    .join(order_totals.alias("order_totals"), F.col("orders.id") == F.col("order_totals.order_id"), "left")
    .select(
        F.col("orders.id"),
        F.col("orders.discount"),
        F.col("orders.shipping_fee"),
        F.col("order_totals.total_amount"),
        F.col("order_totals.first_paid_at"),
    )
)
