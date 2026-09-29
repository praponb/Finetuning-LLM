from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("products.id") == F.col("store_sales.product_id"), "inner")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "inner")
    .select(
        F.col("store_sales.revenue"),
        F.col("products.product_name"),
        F.col("stores.store_name"),
        F.col("stores.region"),
        F.when(F.col("products.category") == "Sports", 1).otherwise(0).alias("is_sports"),
    )
)
