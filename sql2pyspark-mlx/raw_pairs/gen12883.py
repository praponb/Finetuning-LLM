from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("store_sales.store_id"), "inner")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "inner")
    .filter(F.col("products.product_name").like("wi%"))
    .select(
        F.col("store_sales.id"),
        F.col("store_sales.revenue"),
        F.col("store_sales.store_id"),
        F.col("stores.city"),
        F.col("products.category"),
        F.round((F.col("store_sales.units") / F.col("store_sales.revenue")), 2).alias("revenue_per_unit"),
    )
    .orderBy("revenue_per_unit")
)
