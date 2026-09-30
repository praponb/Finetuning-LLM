from pyspark.sql import functions as F

recent_products = df_products.filter(F.col("price").between(370, 1090) & (F.col("supplier_id") == 33))

products_stats = (
    recent_products
    .groupBy("category")
    .agg(F.min("launch_date").alias("min_launch_date"))
)

result = (
    products_stats
    .select("category", "min_launch_date")
    .orderBy("category")
)
