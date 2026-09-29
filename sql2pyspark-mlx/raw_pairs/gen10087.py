from pyspark.sql import functions as F

order_totals = (
    df_payments
    .groupBy("order_id")
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .filter(F.col("t.avg_amount") > 67)
    .select(F.col("o.id"), F.col("o.discount"), F.col("t.avg_amount"))
)
