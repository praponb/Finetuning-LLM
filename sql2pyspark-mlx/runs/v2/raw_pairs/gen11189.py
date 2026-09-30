from pyspark.sql import functions as F

recent_products = (
    df_products
    .filter(F.col("supplier_id").isin([10, 11, 17, 36]) & (F.col("launch_date") <= "2024-04-10"))
)

products_stats = (
    recent_products
    .groupBy(F.month("launch_date").alias("launch_month"))
    .agg((F.max("price") - F.min("price")).alias("price_range"))
)

result = (
    products_stats
    .filter(F.col("price_range") < 53)
    .orderBy(F.col("price_range").desc(), "launch_month")
    .limit(50)
)
