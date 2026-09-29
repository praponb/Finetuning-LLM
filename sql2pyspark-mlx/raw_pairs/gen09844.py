from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_order_items.alias("order_items"), F.col("orders.id") == F.col("order_items.order_id"), "left")
    .filter(F.col("orders.shipping_fee") <= 23)
    .groupBy(F.col("orders.id"), F.col("orders.status"))
    .agg(
        F.count(F.col("order_items.id")).alias("order_item_count"),
        F.round(F.avg(F.col("order_items.quantity")), 2).alias("avg_quantity"),
        F.round(F.avg(F.col("order_items.unit_price")), 2).alias("avg_unit_price"),
    )
    .orderBy(F.col("order_item_count").desc(), "status", "id")
    .limit(3)
)
