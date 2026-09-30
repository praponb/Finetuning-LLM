from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "inner")
    .join(df_suppliers.alias("suppliers"), F.col("suppliers.id") == F.col("products.supplier_id"), "inner")
    .select(
        F.col("order_items.order_id"),
        F.col("products.product_name"),
        F.col("products.launch_date"),
        F.col("suppliers.city"),
    )
    .orderBy(F.col("city").desc())
)
