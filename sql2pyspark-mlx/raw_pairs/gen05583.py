from pyspark.sql import functions as F

order_stats = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.max("cost").alias("max_cost"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("O"), F.col("o.id") == F.col("O.order_id"), "inner")
    .filter(F.col("O.stddev_cost") > 64)
    .select(F.col("o.store_id"), F.col("O.stddev_cost"), F.col("O.max_cost"))
    .orderBy(F.col("max_cost").desc(), "stddev_cost")
)
