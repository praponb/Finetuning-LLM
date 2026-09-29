from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.quarter("launch_date").alias("launch_quarter"))
    .agg(F.max("price").alias("max_price"))
    .filter(F.col("max_price") > 1010)
)
