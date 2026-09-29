from pyspark.sql import functions as F

order_summary = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.count("*").alias("num_payments"),
        F.sum("amount").alias("total_amount"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .select(
        F.col("o.shipping_fee"),
        F.col("o.store_id"),
        F.col("os.num_payments"),
        F.col("os.total_amount"),
    )
)
