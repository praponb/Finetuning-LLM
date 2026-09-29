from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost") >= 410)
    .groupBy("category", F.month("launch_date").alias("launch_month"))
    .agg(
        F.count("cost").alias("cost_count"),
        F.max("launch_date").alias("last_launch_date"),
        F.min("launch_date").alias("min_launch_date"),
    )
)
