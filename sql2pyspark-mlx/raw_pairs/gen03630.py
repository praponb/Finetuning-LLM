from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(
        F.min("launch_date").alias("min_launch_date"),
        F.sum(F.when(F.col("category") == "Fashion", 1).otherwise(0)).alias("fashion_count"),
    )
    .filter(F.col("fashion_count") >= 67)
    .orderBy("brand")
)
