from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_store_sales.alias("store_sales"), F.col("store_sales.product_id") == F.col("products.id"), "left")
    .filter(F.col("store_sales.sale_date") > "2024-06-01")
    .groupBy(F.col("products.id"), F.col("products.category"))
    .agg(
        F.count(F.col("store_sales.id")).alias("store_sale_count"),
        F.min(F.col("store_sales.revenue")).alias("min_revenue"),
    )
    .orderBy(F.col("id").desc())
)
