from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.city"))
    .agg(
        F.count(F.col("o.discount")).alias("discount_count"),
        F.round(F.stddev(F.col("o.amount")), 2).alias("stddev_amount"),
    )
)
