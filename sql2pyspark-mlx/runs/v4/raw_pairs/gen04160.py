from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(F.min("launch_date").alias("first_launch_date"))
)
