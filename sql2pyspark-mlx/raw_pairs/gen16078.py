from pyspark.sql import functions as F

payments_by_order = (
    df_payments
    .filter(F.col("method") == "bank_transfer")
    .groupBy("order_id")
    .agg(
        F.count("*").alias("payment_count"),
        F.count(F.when(F.col("method") == "card", 1)).alias("card_count"),
    )
)

result = (
    df_orders.alias("o")
    .join(payments_by_order.alias("pbo"), F.col("o.id") == F.col("pbo.order_id"), "inner")
    .select(
        F.col("o.shipping_fee"),
        F.col("o.discount"),
        F.col("pbo.payment_count"),
        F.col("pbo.card_count"),
    )
    .orderBy(F.col("discount").desc())
)
