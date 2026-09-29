from pyspark.sql import functions as F

order_totals = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.sum(F.when(F.col("status") == "REFUNDED", 1).otherwise(0)).alias("refunded_count"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "left")
    .select(
        F.col("o.id"),
        F.col("o.order_date"),
        F.col("o.store_id"),
        F.col("ot.refunded_count"),
        F.col("ot.amount_range"),
    )
)
