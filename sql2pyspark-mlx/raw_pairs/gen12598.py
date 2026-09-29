from pyspark.sql import functions as F

order_totals = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.sum(F.when(F.col("method") == "paypal", F.col("amount")).otherwise(0)).alias("paypal_amount"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("O"), F.col("o.id") == F.col("O.order_id"), "left")
    .select(F.col("o.shipping_fee"), F.col("O.paypal_amount"))
)
