from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "inner")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "left")
    .filter(F.col("products.product_name").like("US%"))
    .groupBy(F.col("orders.channel"))
    .agg(
        F.sum((F.col("order_items.unit_price") * F.col("order_items.quantity"))).alias("revenue"),
        F.count(F.col("order_items.quantity")).alias("quantity_count"),
    )
)
