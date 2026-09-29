from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "sale"))
    .groupBy(F.month("launch_date").alias("launch_month"))
    .agg(F.count("price").alias("price_count"))
    .filter(F.col("price_count") >= 7)
    .select("launch_month", "price_count")
    .orderBy(F.col("price_count").desc())
)
