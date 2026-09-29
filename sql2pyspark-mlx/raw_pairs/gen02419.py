from pyspark.sql import functions as F

order_summary = (
    df_payments
    .filter(F.col("order_id").isin([12, 40]))
    .groupBy("order_id")
    .agg(F.min("amount").alias("min_amount"))
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .select(F.col("o.shipping_fee"), F.col("os.min_amount"))
    .orderBy("shipping_fee")
)
