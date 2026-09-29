from pyspark.sql import functions as F

order_summary = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.countDistinct("amount").alias("unique_orders"),
        F.avg("amount").alias("avg_amount"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("os.unique_orders"),
        F.col("os.avg_amount"),
    )
    .orderBy("customer_id")
)
