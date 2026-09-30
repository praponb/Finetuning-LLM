from pyspark.sql import functions as F

order_totals = (
    df_payments
    .filter(F.col("order_id").isin([21, 40]))
    .groupBy("order_id")
    .agg(
        F.min("amount").alias("min_amount"),
        F.max("amount").alias("max_amount"),
    )
)

result = (
    df_orders.alias("orders")
    .join(order_totals.alias("t"), F.col("orders.id") == F.col("t.order_id"), "inner")
    .select(
        F.col("orders.id"),
        F.col("orders.amount"),
        F.col("orders.customer_id"),
        F.col("t.min_amount"),
        F.col("t.max_amount"),
    )
)
