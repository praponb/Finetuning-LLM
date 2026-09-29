from pyspark.sql import functions as F

filtered_products = (
    df_products
    .filter((F.col("launch_date").between("2023-06-25", "2024-10-01") & (F.col("launch_date") < "2023-06-20")) | F.col("stock_qty").between(142, 221))
)

products_stats = (
    filtered_products
    .groupBy("brand")
    .agg(F.min("launch_date").alias("first_launch_date"))
)

result = products_stats.orderBy(F.col("brand").desc())
