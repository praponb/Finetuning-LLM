from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(F.min("launch_date").alias("min_launch_date"))
    .orderBy("brand")
    .limit(1)
)
