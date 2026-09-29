from pyspark.sql import functions as F

order_summary = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.count("amount").alias("amount_count"),
        F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_count"),
    )
)

result = (
    df_orders.alias("orders")
    .join(order_summary.alias("order_summary"), F.col("orders.id") == F.col("order_summary.order_id"), "inner")
    .filter(F.col("order_summary.amount_count") > 10)
    .select(
        F.col("orders.id"),
        F.col("orders.order_date"),
        F.col("order_summary.amount_count"),
        F.col("order_summary.failed_count"),
    )
    .orderBy(F.col("id").desc())
)
