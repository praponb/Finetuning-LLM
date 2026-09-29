from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "outer")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("store_sales.store_id"), "left")
    .groupBy(F.col("products.category"))
    .agg(
        F.min(F.col("store_sales.revenue")).alias("min_revenue"),
        F.countDistinct(F.col("store_sales.units")).alias("unique_products"),
    )
)
