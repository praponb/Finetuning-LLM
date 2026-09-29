from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("products.id") == F.col("order_items.product_id"), "inner")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .filter(F.col("suppliers.city") == "Berlin")
    .select(
        F.col("order_items.id"),
        F.col("order_items.product_id"),
        F.col("order_items.unit_price"),
        F.col("products.category"),
        F.col("suppliers.supplier_name"),
        F.col("suppliers.country"),
    )
    .orderBy(F.col("product_id").desc(), "id")
    .limit(1)
)
