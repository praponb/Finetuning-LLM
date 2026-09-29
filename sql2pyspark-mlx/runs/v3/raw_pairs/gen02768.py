from pyspark.sql import functions as F

order_totals = (
    df_order_items
    .groupBy("order_id")
    .agg(
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
)

result = (
    df_orders.alias("orders")
    .join(order_totals.alias("order_totals"), F.col("orders.id") == F.col("order_totals.order_id"), "left")
    .select(
        F.col("orders.shipping_fee"),
        F.col("order_totals.unit_price_range"),
        F.col("order_totals.avg_quantity"),
    )
)
