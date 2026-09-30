from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "left")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .groupBy(F.col("products.brand"))
    .agg(
        F.min(F.col("order_items.unit_price")).alias("min_unit_price"),
        (F.max(F.col("order_items.quantity")) - F.min(F.col("order_items.quantity"))).alias("quantity_range"),
    )
    .orderBy("brand", "quantity_range")
)
