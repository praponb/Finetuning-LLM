from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(F.min("launch_date").alias("min_launch_date"))
    .orderBy("min_launch_date", "category")
    .limit(10)
)
