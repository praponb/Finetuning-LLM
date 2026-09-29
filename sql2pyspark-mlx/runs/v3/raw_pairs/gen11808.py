from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "inner")
    .select(
        F.col("store_sales.id"),
        F.col("store_sales.revenue"),
        F.col("store_sales.product_id"),
        F.col("store_sales.sale_date"),
        F.col("products.category"),
    )
)
