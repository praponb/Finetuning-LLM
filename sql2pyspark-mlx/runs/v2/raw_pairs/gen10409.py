from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "right")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .groupBy(F.col("o.store_id"))
    .agg(
        F.max(F.col("p.amount")).alias("max_amount"),
        F.round(F.stddev(F.col("p.amount")), 2).alias("stddev_amount"),
    )
    .orderBy(F.col("store_id").desc())
)
