from pyspark.sql import functions as F

order_totals = (
    df_payments
    .groupBy("order_id")
    .agg(F.count("amount").alias("amount_count"))
)

result = (
    df_orders.alias("orders")
    .join(order_totals.alias("t"), F.col("orders.id") == F.col("t.order_id"), "inner")
    .filter(F.col("t.amount_count") >= 4)
    .select(F.col("orders.shipping_fee"), F.col("t.amount_count"))
    .orderBy("amount_count")
)
