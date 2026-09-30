from pyspark.sql import functions as F

order_totals = (
    df_payments
    .groupBy("order_id")
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "inner")
    .filter(F.col("ot.amount_range") >= 54)
    .select(
        F.col("o.id"),
        F.col("o.discount"),
        F.col("o.shipping_fee"),
        F.col("ot.amount_range"),
    )
)
