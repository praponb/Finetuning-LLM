from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.filter(F.col("store_id").isNull()).alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "left_anti")
    .filter(F.col("order_items.unit_price") >= 140)
    .select(
        F.col("order_items.id"),
        F.col("order_items.product_id"),
        F.col("order_items.unit_price"),
        F.col("order_items.order_id"),
    )
    .orderBy(F.col("order_id").desc(), "id")
    .limit(10)
)
