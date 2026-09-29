from pyspark.sql import functions as F

payments_by_order = (
    df_payments
    .filter(F.col("status").isin(["FAILED", "SUCCESS", "REFUNDED"]))
    .groupBy("order_id")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.sum("amount").alias("total_amount"),
    )
)

result = (
    df_orders.alias("o")
    .join(payments_by_order.alias("t"), F.col("o.id") == F.col("t.order_id"), "left")
    .filter(F.col("t.total_amount") > 5000)
    .select(
        F.col("o.customer_id"),
        F.col("o.store_id"),
        F.col("t.stddev_amount"),
        F.col("t.total_amount"),
    )
    .orderBy("stddev_amount")
)
