from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.date_format("launch_date", "yyyy-MM").alias("launch_month"), "category")
    .agg(
        F.min("launch_date").alias("first_launch_date"),
        F.count("*").alias("product_count"),
    )
    .filter(F.col("product_count") >= 5)
    .orderBy(F.col("first_launch_date").desc())
)
