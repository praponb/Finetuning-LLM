from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "left")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("order_items.order_id"), "inner")
    .filter(F.col("orders.channel").isin(["web", "mobile", "partner"]))
    .select(
        F.col("order_items.id"),
        F.col("order_items.quantity"),
        F.col("order_items.product_id"),
        F.col("products.supplier_id"),
        F.col("products.product_name"),
        F.col("orders.store_id"),
    )
    .orderBy("quantity", "id")
    .limit(1)
)
