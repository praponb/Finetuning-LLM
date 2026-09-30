from pyspark.sql import functions as F

valid_products = df_products.filter(F.col("cost").isNotNull())

launch_year_summary = (
    valid_products
    .groupBy(F.year("launch_date").alias("launch_year"))
    .agg(F.min("launch_date").alias("min_launch_date"))
)

result = launch_year_summary.orderBy("min_launch_date")
