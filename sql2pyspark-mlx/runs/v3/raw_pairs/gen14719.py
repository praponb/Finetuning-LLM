from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.filter(F.col("amount") >= 1420).alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "left_semi")
    .filter(F.col("order_items.order_id").isin([32, 37, 39]))
    .select(
        F.col("order_items.id"),
        F.col("order_items.order_id"),
        F.col("order_items.product_id"),
        F.col("order_items.quantity"),
    )
)
