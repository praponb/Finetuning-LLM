from pyspark.sql import functions as F

order_totals = (
    df_order_items
    .filter(F.col("unit_price") >= 270)
    .groupBy("order_id")
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
)

result = (
    df_orders.alias("orders")
    .join(order_totals.alias("order_totals"), F.col("orders.id") == F.col("order_totals.order_id"), "left")
    .select(
        F.col("orders.shipping_fee"),
        F.col("orders.customer_id"),
        F.col("order_totals.stddev_quantity"),
    )
)
