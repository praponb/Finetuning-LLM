from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_store_sales.alias("store_sales"), F.col("products.id") == F.col("store_sales.product_id"), "inner")
    .filter(F.array_contains(F.col("products.tags"), "new"))
    .select(
        F.col("products.price"),
        F.col("products.brand"),
        F.col("store_sales.product_id"),
        F.least(F.col("store_sales.revenue"), F.col("store_sales.units")).alias("least_revenue_units"),
    )
)
