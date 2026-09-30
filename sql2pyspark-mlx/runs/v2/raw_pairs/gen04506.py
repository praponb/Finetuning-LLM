from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "right")
    .groupBy(F.col("s.region"))
    .agg(
        F.min(F.col("o.order_date")).alias("first_order_date"),
        F.round(F.stddev(F.col("o.discount")), 2).alias("stddev_discount"),
    )
    .filter(F.col("stddev_discount") >= 49)
    .orderBy(F.col("first_order_date").desc())
)
