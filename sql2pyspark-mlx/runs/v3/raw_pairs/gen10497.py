from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "inner")
    .join(df_suppliers.alias("suppliers"), F.col("suppliers.id") == F.col("products.supplier_id"), "inner")
    .filter(F.col("suppliers.rating") >= 3.5)
    .select(
        F.col("store_sales.id"),
        F.col("store_sales.store_id"),
        F.col("products.product_name"),
        F.col("suppliers.city"),
    )
    .orderBy("city", F.col("store_id").desc())
)
