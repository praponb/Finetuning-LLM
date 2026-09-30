from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(
        F.max("launch_date").alias("last_launch_date"),
        F.count(F.when(F.col("category") == "Fashion", 1)).alias("fashion_count"),
    )
    .orderBy("category")
)
