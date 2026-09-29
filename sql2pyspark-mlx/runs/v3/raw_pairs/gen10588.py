from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_payments.alias("payments"), F.col("orders.id") == F.col("payments.order_id"), "left")
    .filter(F.col("payments.amount") <= 1260)
    .groupBy(F.col("orders.channel"))
    .agg(
        F.count(F.col("payments.id")).alias("payment_count"),
        F.countDistinct(F.col("payments.amount")).alias("unique_statuss"),
        F.round(F.stddev(F.col("payments.amount")), 2).alias("stddev_amount"),
    )
    .orderBy("channel")
)
