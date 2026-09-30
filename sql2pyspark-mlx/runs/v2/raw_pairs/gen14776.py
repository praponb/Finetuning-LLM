from pyspark.sql import functions as F

order_summary = (
    df_payments
    .filter(F.col("amount") < 1330)
    .groupBy("order_id")
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("O"), F.col("o.id") == F.col("O.order_id"), "inner")
    .filter(F.col("O.amount_range") >= 54)
    .select(F.col("o.order_date"), F.col("O.amount_range"))
    .orderBy("order_date")
)
