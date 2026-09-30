from pyspark.sql import functions as F

order_totals = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.count(F.when(F.col("method") == "cash", 1)).alias("cash_count"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .filter(F.col("t.avg_amount") >= 430)
    .select(
        F.col("o.id"),
        F.col("o.discount"),
        F.col("o.amount"),
        F.col("t.avg_amount"),
        F.col("t.cash_count"),
    )
    .orderBy(F.col("discount").desc(), F.col("cash_count").desc())
)
