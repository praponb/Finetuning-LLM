from pyspark.sql import functions as F

order_summary = (
    df_payments
    .filter(F.col("order_id").isNull())
    .groupBy("order_id")
    .agg(
        F.count("amount").alias("amount_count"),
        F.count(F.when(F.col("method") == "promptpay", 1)).alias("promptpay_count"),
    )
)

result = (
    df_orders.alias("orders")
    .join(order_summary.alias("order_summary"), F.col("orders.id") == F.col("order_summary.order_id"), "left")
    .filter(F.col("order_summary.promptpay_count") > 9)
    .select(
        F.col("orders.id"),
        F.col("orders.customer_id"),
        F.col("order_summary.amount_count"),
        F.col("order_summary.promptpay_count"),
    )
    .orderBy("promptpay_count")
)
