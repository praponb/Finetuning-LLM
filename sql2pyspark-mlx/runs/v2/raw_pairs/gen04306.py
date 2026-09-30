from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_store_sales.alias("store_sales"), F.col("products.id") == F.col("store_sales.product_id"), "left")
    .filter((F.col("store_sales.units") >= 12) & (F.col("store_sales.product_id") == 5))
    .select(
        F.col("products.product_name"),
        F.col("store_sales.store_id"),
        F.col("store_sales.sale_date"),
        F.col("store_sales.product_id"),
    )
    .orderBy(F.col("sale_date").desc())
)
