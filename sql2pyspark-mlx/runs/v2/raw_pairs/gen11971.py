from pyspark.sql import functions as F

result = (
    df_products
    .rollup("category", "brand")
    .agg(F.min("launch_date").alias("first_launch_date"))
    .orderBy("first_launch_date")
)
