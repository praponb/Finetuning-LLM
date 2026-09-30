from pyspark.sql import functions as F

order_summary = (
    df_payments
    .filter(F.col("order_id").isNull())
    .groupBy("order_id")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.sum("amount").alias("total_amount"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .filter(F.col("os.total_amount") > 6600)
    .select(F.col("o.channel"), F.col("os.avg_amount"), F.col("os.total_amount"))
    .orderBy(F.col("channel").desc())
)
