from pyspark.sql import functions as F

order_items_by_order = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
    )
)

result = (
    df_orders.alias("orders")
    .join(order_items_by_order.alias("t"), F.col("orders.id") == F.col("t.order_id"), "inner")
    .filter(F.col("t.total_quantity") > 19)
    .select(
        F.col("orders.id"),
        F.col("orders.channel"),
        F.col("orders.store_id"),
        F.col("t.total_quantity"),
        F.col("t.unit_price_range"),
    )
    .orderBy("total_quantity", "unit_price_range")
)
