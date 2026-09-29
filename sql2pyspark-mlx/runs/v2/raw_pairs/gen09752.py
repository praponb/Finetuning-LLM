from pyspark.sql import functions as F

payments_by_order = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.sum(F.when(F.col("method") == "cash", 1).otherwise(0)).alias("cash_count"),
        F.count("*").alias("row_count"),
    )
)

result = (
    df_orders.alias("o")
    .join(payments_by_order.alias("pbo"), F.col("o.id") == F.col("pbo.order_id"), "inner")
    .select(F.col("o.channel"), F.col("pbo.cash_count"), F.col("pbo.row_count"))
    .orderBy(F.col("row_count").desc())
)
