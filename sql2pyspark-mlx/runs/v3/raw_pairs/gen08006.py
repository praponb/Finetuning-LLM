from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_store_sales.alias("store_sales"), F.col("store_sales.product_id") == F.col("products.id"), "left")
    .filter(F.col("store_sales.store_id").isin([1, 2, 3]) | (F.col("store_sales.revenue") <= 2800))
    .select(
        F.col("products.supplier_id"),
        F.col("products.cost"),
        F.col("products.price"),
        F.col("store_sales.store_id"),
        F.col("store_sales.revenue"),
    )
)
