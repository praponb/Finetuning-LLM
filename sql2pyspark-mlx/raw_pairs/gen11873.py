from pyspark.sql import functions as F

order_summary = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.count("amount").alias("amount_count"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("t"), F.col("o.id") == F.col("t.order_id"), "left")
    .filter(F.col("t.stddev_amount") > 27)
    .select(
        F.col("o.id"),
        F.col("o.amount"),
        F.col("t.amount_count"),
        F.col("t.stddev_amount"),
    )
    .orderBy("stddev_amount", F.col("amount_count").desc())
)
