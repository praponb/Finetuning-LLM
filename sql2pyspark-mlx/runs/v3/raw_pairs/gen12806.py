from pyspark.sql import functions as F

shipments_by_order = (
    df_shipments
    .groupBy("order_id")
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
    )
)

result = (
    df_orders.alias("o")
    .join(shipments_by_order.alias("sbo"), F.col("o.id") == F.col("sbo.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("o.order_date"),
        F.col("sbo.cost_range"),
        F.col("sbo.stddev_cost"),
    )
)
