from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "left")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .select(
        F.col("order_items.product_id"),
        F.col("products.product_name"),
        F.col("suppliers.country"),
        F.initcap(F.trim(F.col("suppliers.supplier_name"))).alias("supplier_name_title"),
    )
    .orderBy(F.col("product_name").desc())
)
