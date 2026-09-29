from pyspark.sql import functions as F

filtered_products = df_products.filter(F.col("stock_qty") == 346)

summary = (
    filtered_products
    .groupBy(F.quarter("launch_date").alias("launch_quarter"))
    .agg(F.sum(F.when(F.col("brand") == "Stark", 1).otherwise(0)).alias("stark_count"))
)

result = (
    summary
    .filter(F.col("stark_count") >= 75)
    .select("launch_quarter", "stark_count")
    .orderBy(F.col("stark_count").desc())
)
