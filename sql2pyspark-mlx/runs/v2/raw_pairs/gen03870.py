from pyspark.sql import functions as F

payments_by_order = (
    df_payments
    .filter(F.col("status") == "REFUNDED")
    .groupBy("order_id")
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.max("amount").alias("max_amount"),
    )
)

result = (
    df_orders.alias("o")
    .join(payments_by_order.alias("pbo"), F.col("o.id") == F.col("pbo.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.shipping_fee"),
        F.col("pbo.amount_range"),
        F.col("pbo.max_amount"),
    )
    .orderBy(F.col("shipping_fee").desc(), "amount_range")
)
