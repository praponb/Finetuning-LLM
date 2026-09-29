from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .groupBy(F.col("o.channel"))
    .agg(
        F.round(F.stddev(F.col("p.amount")), 2).alias("stddev_amount"),
        F.countDistinct(F.col("p.amount")).alias("unique_orders"),
    )
)
