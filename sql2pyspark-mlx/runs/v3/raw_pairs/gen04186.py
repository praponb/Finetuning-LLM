from pyspark.sql import functions as F

payments_by_order = (
    df_payments
    .filter(F.col("status") == "REFUNDED")
    .groupBy("order_id")
    .agg(F.countDistinct("amount").alias("unique_orders"))
)

result = (
    df_orders.alias("orders")
    .join(payments_by_order.alias("payments_by_order"), F.col("orders.id") == F.col("payments_by_order.order_id"), "inner")
    .filter(F.col("payments_by_order.unique_orders") > 6)
    .select(
        F.col("orders.id"),
        F.col("orders.customer_id"),
        F.col("orders.channel"),
        F.col("payments_by_order.unique_orders"),
    )
    .orderBy("id", F.col("unique_orders").desc())
)
