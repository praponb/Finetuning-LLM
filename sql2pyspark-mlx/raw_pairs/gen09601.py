from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_store_sales.filter(F.col("sale_date").between("2025-03-01", "2025-06-01")).alias("store_sales"), F.col("products.id") == F.col("store_sales.product_id"), "left_anti")
    .select(
        F.col("products.id"),
        F.col("products.brand"),
        F.col("products.product_name"),
        F.col("products.cost"),
    )
)
