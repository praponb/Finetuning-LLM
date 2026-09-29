from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "inner")
    .select(
        F.col("store_sales.id"),
        F.col("store_sales.product_id"),
        F.col("stores.city"),
    )
)
