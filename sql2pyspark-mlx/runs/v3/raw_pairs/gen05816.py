from pyspark.sql import functions as F

payments_by_order = (
    df_payments
    .groupBy("order_id")
    .agg(F.min("amount").alias("min_amount"))
)

result = (
    df_orders.alias("o")
    .join(payments_by_order.alias("t"), F.col("o.id") == F.col("t.order_id"), "left")
    .filter(F.col("t.min_amount") >= 1510)
    .select(F.col("o.customer_id"), F.col("t.min_amount"))
    .orderBy(F.col("customer_id").desc())
)
