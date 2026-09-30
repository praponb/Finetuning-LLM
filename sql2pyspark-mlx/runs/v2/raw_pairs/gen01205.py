from pyspark.sql import functions as F

base = df_products.filter(F.col("product_name").like("%Shoe%"))

products_stats = (
    base
    .groupBy("category")
    .agg(
        F.max("launch_date").alias("latest_launch_date"),
        F.min("launch_date").alias("min_launch_date"),
    )
)

result = products_stats
