from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "inner")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "outer")
    .groupBy(F.col("products.brand"))
    .agg(
        F.count(F.col("order_items.unit_price")).alias("unit_price_count"),
        F.max(F.col("order_items.unit_price")).alias("max_unit_price"),
    )
    .filter(F.col("max_unit_price") >= 230)
    .orderBy(F.col("max_unit_price").desc())
)
