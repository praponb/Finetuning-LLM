from pyspark.sql import functions as F

order_totals = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        (F.max("cost") - F.min("cost")).alias("cost_range"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("ot.stddev_cost"),
        F.col("ot.cost_range"),
    )
)
