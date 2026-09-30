from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_store_sales.filter(F.col("units").isNotNull()).alias("store_sales"), F.col("products.id") == F.col("store_sales.product_id"), "left_semi")
    .select(
        F.col("products.id"),
        F.col("products.supplier_id"),
        F.col("products.category"),
    )
)
