from pyspark.sql import functions as F

filtered_products = (
    df_products
    .filter(F.col("product_name").like("%Bottl%") | (F.col("brand") == "Initech"))
    .withColumn("tags_count", F.size("tags"))
)

launch_month_summary = (
    filtered_products
    .groupBy(F.date_format("launch_date", "yyyy-MM").alias("launch_month"))
    .agg(F.min("launch_date").alias("min_launch_date"))
)

result = launch_month_summary.orderBy("launch_month")
