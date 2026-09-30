from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("store_sales.store_id"), "inner")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "left")
    .select(
        F.col("store_sales.id"),
        F.col("store_sales.revenue"),
        F.col("store_sales.sale_date"),
        F.col("stores.region"),
        F.col("products.category"),
        F.col("products.price"),
        F.coalesce(F.col("stores.store_name"), F.lit("N/A")).alias("store_name_filled"),
    )
    .orderBy("price")
)
