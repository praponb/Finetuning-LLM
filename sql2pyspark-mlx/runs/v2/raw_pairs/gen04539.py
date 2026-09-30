from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_order_items.alias("order_items"), F.col("order_items.order_id") == F.col("orders.id"), "inner")
    .filter((F.col("orders.order_date") < "2024-02-28") & (F.col("order_items.unit_price") < 110))
    .select(
        F.col("orders.order_date"),
        F.col("orders.channel"),
        F.col("orders.discount"),
        F.col("order_items.product_id"),
        F.col("order_items.unit_price"),
    )
)
