from pyspark.sql import functions as F

order_summary = (
    df_payments
    .filter(F.col("method").isNotNull())
    .groupBy("order_id")
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
)

result = (
    df_orders.alias("orders")
    .join(order_summary.alias("order_summary"), F.col("orders.id") == F.col("order_summary.order_id"), "left")
    .filter(F.col("order_summary.amount_range") > 28)
    .select(
        F.col("orders.channel"),
        F.col("order_summary.amount_range"),
        F.col("order_summary.avg_amount"),
    )
    .orderBy("channel", "avg_amount")
)
