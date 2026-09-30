from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "inner")
    .select(
        F.col("order_items.id"),
        F.col("order_items.quantity"),
        F.col("order_items.unit_price"),
        F.col("orders.discount"),
    )
    .orderBy(F.col("quantity").desc())
)
