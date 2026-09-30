from pyspark.sql import functions as F

order_totals = (
    df_payments
    .filter(F.col("amount") >= 850)
    .groupBy("order_id")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.min("amount").alias("min_amount"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "inner")
    .filter(F.col("ot.min_amount") > 570)
    .select(
        F.col("o.id"),
        F.col("o.discount"),
        F.col("ot.stddev_amount"),
        F.col("ot.min_amount"),
    )
    .orderBy("min_amount")
)
