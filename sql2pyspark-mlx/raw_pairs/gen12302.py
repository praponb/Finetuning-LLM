from pyspark.sql import functions as F

order_totals = (
    df_payments
    .groupBy("order_id")
    .agg(F.max("amount").alias("max_amount"))
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .filter(F.col("t.max_amount") >= 500)
    .select(F.col("o.id"), F.col("o.amount"), F.col("t.max_amount"))
    .orderBy(F.col("max_amount").desc())
)
