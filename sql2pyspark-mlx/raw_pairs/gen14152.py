from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date").between("2023-06-01", "2025-07-01"))
    .groupBy(F.quarter("launch_date").alias("launch_quarter"), "brand")
    .agg(
        F.sum("cost").alias("total_cost"),
        F.max("launch_date").alias("latest_launch_date"),
    )
    .orderBy("total_cost", "launch_quarter", "brand")
    .limit(50)
)
