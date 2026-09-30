from pyspark.sql import functions as F

order_summary = (
    df_order_items
    .groupBy("order_id")
    .agg(F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"))
)

result = (
    df_orders.alias("orders")
    .join(order_summary.alias("order_summary"), F.col("orders.id") == F.col("order_summary.order_id"), "inner")
    .filter(F.col("order_summary.stddev_unit_price") > 66)
    .select(
        F.col("orders.shipping_fee"),
        F.col("orders.channel"),
        F.col("order_summary.stddev_unit_price"),
    )
)
