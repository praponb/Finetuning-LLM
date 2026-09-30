from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.quarter("launch_date").alias("launch_quarter"))
    .agg(F.max("cost").alias("max_cost"))
    .orderBy(F.col("max_cost").desc(), "launch_quarter")
    .limit(50)
)
