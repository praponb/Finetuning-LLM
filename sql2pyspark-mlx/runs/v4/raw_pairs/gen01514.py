from pyspark.sql import functions as F

order_stats = (
    df_shipments
    .groupBy("order_id")
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("O"), F.col("o.id") == F.col("O.order_id"), "left")
    .filter(F.col("O.cost_range") >= 80)
    .select(
        F.col("o.id"),
        F.col("o.order_date"),
        F.col("O.cost_range"),
        F.col("O.stddev_cost"),
    )
    .orderBy(F.col("cost_range").desc())
)
