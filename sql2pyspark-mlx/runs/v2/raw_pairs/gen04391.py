from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("sqft") >= 15775) & (F.col("opened_date") > "2025-01-31") & (F.col("opened_date") <= "2024-01-25"))
    .groupBy("city", F.month("opened_date").alias("opened_month"))
    .agg(F.count(F.when(F.col("city") == "Berlin", 1)).alias("berlin_count"))
    .filter(F.col("berlin_count") >= 3)
)
