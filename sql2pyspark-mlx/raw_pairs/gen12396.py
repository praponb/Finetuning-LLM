from pyspark.sql import functions as F

order_totals = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.min("amount").alias("min_amount"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "inner")
    .filter(F.col("ot.avg_amount") >= 70)
    .select(F.col("o.discount"), F.col("ot.min_amount"), F.col("ot.avg_amount"))
    .orderBy("min_amount")
)
