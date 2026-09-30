from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "inner")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "left")
    .filter(F.col("store_sales.product_id").isin([1, 2, 8]) | (F.year(F.col("stores.opened_date")) >= 2024))
    .select(
        F.col("store_sales.id"),
        F.col("store_sales.store_id"),
        F.col("store_sales.units"),
        F.col("stores.region"),
        F.col("stores.store_name"),
        F.col("products.price"),
    )
)
