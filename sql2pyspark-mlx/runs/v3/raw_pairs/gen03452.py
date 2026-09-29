from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "inner")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "inner")
    .filter(F.col("stores.city").isin(["New York", "Chiang Mai"]))
    .select(
        F.col("store_sales.id"),
        F.col("store_sales.revenue"),
        F.col("stores.city"),
        F.col("stores.store_name"),
        F.col("products.cost"),
    )
)
