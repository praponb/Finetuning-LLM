from pyspark.sql import functions as F

result = (
    df_products
    .filter(~F.col("brand").isin(["Globex", "Stark"]) | F.col("launch_date").between("2024-08-13", "2025-03-01"))
    .groupBy(F.quarter("launch_date").alias("launch_quarter"))
    .agg(
        F.sum("stock_qty").alias("total_stock_qty"),
        F.count(F.when(F.col("brand") == "Initech", 1)).alias("initech_count"),
    )
)
