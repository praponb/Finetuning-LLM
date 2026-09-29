from pyspark.sql import functions as F

order_summary = (
    df_payments
    .groupBy("order_id")
    .agg(F.count("amount").alias("amount_count"))
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("O"), F.col("o.id") == F.col("O.order_id"), "inner")
    .filter(F.col("O.amount_count") > 7)
    .select(
        F.col("o.id"),
        F.col("o.status"),
        F.col("o.order_date"),
        F.col("O.amount_count"),
    )
    .orderBy(F.col("amount_count").desc(), "id")
    .limit(1)
)
