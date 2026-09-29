from pyspark.sql import functions as F

order_totals = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.avg("amount").alias("avg_amount"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "inner")
    .select(F.col("o.discount"), F.col("ot.total_amount"), F.col("ot.avg_amount"))
    .orderBy(F.col("avg_amount").desc())
)
