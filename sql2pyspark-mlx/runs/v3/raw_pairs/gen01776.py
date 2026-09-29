from pyspark.sql import functions as F

order_totals = (
    df_order_items
    .filter(F.col("unit_price") > 210)
    .groupBy("order_id")
    .agg(F.max("unit_price").alias("max_unit_price"))
)

result = (
    df_orders.alias("orders")
    .join(order_totals.alias("order_totals"), F.col("orders.id") == F.col("order_totals.order_id"), "left")
    .select(
        F.col("orders.id"),
        F.col("orders.shipping_fee"),
        F.col("order_totals.max_unit_price"),
    )
)
