from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "inner")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "outer")
    .select(
        F.col("store_sales.id"),
        F.col("store_sales.store_id"),
        F.col("stores.region"),
        F.col("products.category"),
        F.date_add(F.col("products.launch_date"), 64).alias("launch_date_plus_64d"),
    )
    .orderBy("category")
)
