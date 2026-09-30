from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "outer")
    .filter(F.col("store_sales.store_id").isin([1, 2, 5, 6]))
    .groupBy(F.month(F.col("products.launch_date")).alias("launch_month"))
    .agg(F.min(F.col("store_sales.revenue")).alias("min_revenue"))
)
