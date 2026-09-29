from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date") >= "2025-03-15")
    .groupBy("category")
    .agg(
        F.min("launch_date").alias("first_launch_date"),
        F.sum(F.when(F.col("brand") == "Umbrella", 1).otherwise(0)).alias("umbrella_count"),
    )
)
