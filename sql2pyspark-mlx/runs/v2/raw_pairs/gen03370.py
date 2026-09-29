from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "outer")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "inner")
    .groupBy(F.col("products.supplier_id"), F.col("products.brand"))
    .agg(F.max(F.col("order_items.quantity")).alias("max_quantity"))
    .filter(F.col("max_quantity") > 8.1)
    .orderBy(F.col("max_quantity").desc(), "supplier_id")
)
