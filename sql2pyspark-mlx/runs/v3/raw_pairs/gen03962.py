from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "inner")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "left")
    .filter((F.col("suppliers.city") == "New York") | (F.col("store_sales.store_id") == 1))
    .select(
        F.col("store_sales.id"),
        F.col("store_sales.product_id"),
        F.col("store_sales.store_id"),
        F.col("products.cost"),
        F.col("products.brand"),
        F.col("suppliers.supplier_name"),
        F.col("suppliers.city"),
    )
    .orderBy("product_id")
)
