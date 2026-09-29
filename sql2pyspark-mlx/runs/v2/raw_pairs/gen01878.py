from pyspark.sql import functions as F

order_summary = (
    df_payments
    .filter(F.col("status").isin(["SUCCESS", "REFUNDED"]))
    .groupBy("order_id")
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.order_date"),
        F.col("o.discount"),
        F.col("os.avg_amount"),
    )
    .orderBy(F.col("avg_amount").desc(), "id")
    .limit(10)
)
