from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("launch_date") > "2025-08-31") & (F.upper(F.trim("product_name")) == "SMART WATCH"))
    .groupBy("category", "brand")
    .agg(F.min("launch_date").alias("first_launch_date"))
    .orderBy("first_launch_date")
)
