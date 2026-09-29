from pyspark.sql import functions as F

payments_by_order = (
    df_payments
    .groupBy("order_id")
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)

result = (
    df_orders.alias("o")
    .join(payments_by_order.alias("pbo"), F.col("o.id") == F.col("pbo.order_id"), "inner")
    .filter(F.col("pbo.avg_amount") > 28)
    .select(F.col("o.order_date"), F.col("o.discount"), F.col("pbo.avg_amount"))
    .orderBy("discount", "avg_amount")
)
