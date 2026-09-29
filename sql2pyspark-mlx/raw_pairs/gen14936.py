from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "inner")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .filter(F.col("store_sales.sale_date") > "2024-03-16")
    .select(
        F.col("store_sales.id"),
        F.col("store_sales.units"),
        F.col("store_sales.revenue"),
        F.col("products.supplier_id"),
        F.col("products.category"),
        F.col("suppliers.country"),
    )
)
