from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "inner")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "inner")
    .filter(F.col("store_sales.sale_date") <= "2024-03-01")
    .select(
        F.col("store_sales.id"),
        F.col("store_sales.product_id"),
        F.col("stores.city"),
        F.col("products.supplier_id"),
    )
    .orderBy(F.col("id").desc())
)
