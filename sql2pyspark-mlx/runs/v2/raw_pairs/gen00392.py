from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category", F.quarter("launch_date").alias("launch_quarter"))
    .agg(F.count("*").alias("num_products"))
    .filter(F.col("num_products") >= 6)
    .select("category", "launch_quarter", "num_products")
    .orderBy("num_products", "category", "launch_quarter")
    .limit(3)
)
