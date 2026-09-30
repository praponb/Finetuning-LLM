from pyspark.sql import functions as F

order_stats = (
    df_payments
    .groupBy("order_id")
    .agg(F.countDistinct("amount").alias("unique_statuss"))
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("O"), F.col("o.id") == F.col("O.order_id"), "inner")
    .select(F.col("o.order_date"), F.col("o.amount"), F.col("O.unique_statuss"))
    .orderBy(F.col("unique_statuss").desc())
)
