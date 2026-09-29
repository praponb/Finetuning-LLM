from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("s.id") == F.col("o.store_id"), "left")
    .groupBy(F.col("s.id"), F.col("s.city"))
    .agg(
        F.count(F.col("o.id")).alias("order_count"),
        F.round(F.stddev(F.col("o.discount")), 2).alias("stddev_discount"),
        F.min(F.col("o.order_date")).alias("min_order_date"),
    )
)
