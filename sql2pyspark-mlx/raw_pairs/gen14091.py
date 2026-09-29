from pyspark.sql import functions as F

order_summary = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.countDistinct("amount").alias("unique_orders"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .filter(F.col("os.unique_orders") > 8)
    .select(F.col("o.order_date"), F.col("os.stddev_amount"), F.col("os.unique_orders"))
    .orderBy("order_date")
)
