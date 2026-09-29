from pyspark.sql import functions as F

order_stats = (
    df_payments
    .groupBy("order_id")
    .agg(F.max("amount").alias("max_amount"))
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("t"), F.col("o.id") == F.col("t.order_id"), "left")
    .filter(F.col("t.max_amount") >= 940)
    .select(F.col("o.id"), F.col("o.shipping_fee"), F.col("t.max_amount"))
)
