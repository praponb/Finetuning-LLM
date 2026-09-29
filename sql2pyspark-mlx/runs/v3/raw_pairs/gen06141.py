from pyspark.sql import functions as F

payments_by_order = (
    df_payments
    .filter(F.col("method") == "cash")
    .groupBy("order_id")
    .agg(
        F.sum("amount").alias("total_amount"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
)

result = (
    df_orders.alias("orders")
    .join(payments_by_order.alias("payments_by_order"), F.col("orders.id") == F.col("payments_by_order.order_id"), "inner")
    .filter(F.col("payments_by_order.amount_range") >= 64)
    .select(
        F.col("orders.id"),
        F.col("orders.shipping_fee"),
        F.col("payments_by_order.total_amount"),
        F.col("payments_by_order.amount_range"),
    )
    .orderBy(F.col("id").desc())
)
