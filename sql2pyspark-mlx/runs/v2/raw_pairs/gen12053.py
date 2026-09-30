from pyspark.sql import functions as F

payments_by_order = (
    df_payments
    .filter(F.col("order_id") == 14)
    .groupBy("order_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("method") == "paypal", 1).otherwise(0))) / F.count("*")), 1).alias("pct_paypal"),
        F.count("*").alias("payment_count"),
    )
)

result = (
    df_orders.alias("o")
    .join(payments_by_order.alias("P"), F.col("o.id") == F.col("P.order_id"), "left")
    .filter(F.col("P.pct_paypal") >= 79)
    .select(F.col("o.shipping_fee"), F.col("P.pct_paypal"), F.col("P.payment_count"))
    .orderBy(F.col("payment_count").desc(), F.col("shipping_fee").desc())
)
