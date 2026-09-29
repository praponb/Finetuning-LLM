from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("products.id") == F.col("store_sales.product_id"), "inner")
    .filter(F.array_contains(F.col("products.tags"), "premium"))
    .select(
        F.col("store_sales.id"),
        F.col("store_sales.revenue"),
        F.col("store_sales.product_id"),
        F.col("products.product_name"),
        F.col("products.supplier_id"),
    )
)
