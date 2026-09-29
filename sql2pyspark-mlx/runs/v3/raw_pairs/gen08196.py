from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(F.max("launch_date").alias("latest_launch_date"))
)
