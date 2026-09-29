from pyspark.sql import functions as F

order_totals = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.min("amount").alias("min_amount"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "left")
    .filter(F.col("ot.amount_range") > 55)
    .select(
        F.col("o.customer_id"),
        F.col("o.shipping_fee"),
        F.col("ot.min_amount"),
        F.col("ot.amount_range"),
    )
    .orderBy(F.col("min_amount").desc())
)
