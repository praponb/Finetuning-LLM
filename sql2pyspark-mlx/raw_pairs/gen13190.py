from pyspark.sql import functions as F

order_totals = (
    df_payments
    .filter(F.col("status") != "FAILED")
    .groupBy("order_id")
    .agg(F.avg("amount").alias("avg_amount"))
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "left")
    .filter(F.col("ot.avg_amount") > 410)
    .select(F.col("o.amount"), F.col("ot.avg_amount"))
)
