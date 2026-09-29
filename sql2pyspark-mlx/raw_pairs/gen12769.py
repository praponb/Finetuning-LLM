from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.quarter("launch_date").alias("launch_quarter"), "brand")
    .agg(F.count("*").alias("num_products"))
    .filter(F.col("num_products") > 5)
    .orderBy(F.col("brand").desc(), F.col("num_products").desc())
)
