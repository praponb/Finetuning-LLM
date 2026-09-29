from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "inner")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "inner")
    .filter(F.col("order_items.product_id").isNotNull())
    .groupBy(F.col("products.category"), F.col("products.supplier_id"))
    .agg(
        F.count(F.col("order_items.unit_price")).alias("unit_price_count"),
        F.sum(F.col("order_items.unit_price")).alias("total_unit_price"),
    )
    .filter(F.col("unit_price_count") >= 4)
)
