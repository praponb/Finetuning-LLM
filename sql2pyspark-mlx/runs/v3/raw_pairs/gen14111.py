from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_store_sales.filter(F.col("store_id").isin([1, 5])).alias("store_sales"), F.col("products.id") == F.col("store_sales.product_id"), "left_semi")
    .filter(F.array_contains(F.col("products.tags"), "eco"))
    .select(
        F.col("products.id"),
        F.col("products.brand"),
        F.col("products.price"),
        F.col("products.cost"),
    )
)
