from pyspark.sql import functions as F

order_totals = (
    df_payments
    .groupBy("order_id")
    .agg(F.avg("amount").alias("avg_amount"))
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "inner")
    .filter(F.col("ot.avg_amount") > 750)
    .select(F.col("o.status"), F.col("ot.avg_amount"))
    .orderBy("status", F.col("avg_amount").desc())
)
