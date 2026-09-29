from pyspark.sql import functions as F

order_summary = (
    df_payments
    .groupBy("order_id")
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)

result = (
    df_orders.alias("orders")
    .join(order_summary.alias("order_summary"), F.col("orders.id") == F.col("order_summary.order_id"), "inner")
    .select(
        F.col("orders.id"),
        F.col("orders.channel"),
        F.col("order_summary.avg_amount"),
    )
    .orderBy(F.col("id").desc())
)
