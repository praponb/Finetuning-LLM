from pyspark.sql import functions as F

payments_by_order = (
    df_payments
    .filter(F.hour("paid_at").between(12, 18))
    .groupBy("order_id")
    .agg(
        F.sum(F.when(F.col("method") == "paypal", F.col("amount")).otherwise(0)).alias("paypal_amount"),
    )
)

result = (
    df_orders.alias("o")
    .join(payments_by_order.alias("pbo"), F.col("o.id") == F.col("pbo.order_id"), "inner")
    .filter(F.col("pbo.paypal_amount") >= 45)
    .select(F.col("o.id"), F.col("o.channel"), F.col("pbo.paypal_amount"))
    .orderBy(F.col("id").desc())
    .limit(10)
)
