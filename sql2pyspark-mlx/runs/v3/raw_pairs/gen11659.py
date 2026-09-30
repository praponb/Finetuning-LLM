from pyspark.sql import functions as F

order_totals = (
    df_payments
    .groupBy("order_id")
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "left")
    .filter(F.col("ot.stddev_amount") >= 34)
    .select(F.col("o.id"), F.col("o.shipping_fee"), F.col("ot.stddev_amount"))
    .orderBy(F.col("stddev_amount").desc())
)
