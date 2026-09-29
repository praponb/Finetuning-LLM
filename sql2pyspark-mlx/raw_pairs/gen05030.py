from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("products.id") == F.col("store_sales.product_id"), "inner")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "outer")
    .filter(F.array_contains(F.col("products.tags"), "eco"))
    .groupBy(F.month(F.col("products.launch_date")).alias("launch_month"))
    .agg(F.min(F.col("store_sales.revenue")).alias("min_revenue"))
    .filter(F.col("min_revenue") >= 2800)
    .orderBy(F.col("launch_month").desc())
    .limit(10)
)
