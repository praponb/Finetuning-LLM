from pyspark.sql import functions as F

order_stats = (
    df_payments
    .filter(F.col("amount").isNotNull())
    .groupBy("order_id")
    .agg(F.countDistinct("amount").alias("unique_methods"))
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .select(F.col("o.customer_id"), F.col("o.amount"), F.col("os.unique_methods"))
    .orderBy(F.col("amount").desc())
)
