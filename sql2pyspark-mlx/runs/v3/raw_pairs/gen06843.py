from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(
        F.count("*").alias("row_count"),
        F.min("launch_date").alias("first_launch_date"),
    )
    .filter(F.col("row_count") >= 8)
    .orderBy(F.col("category").desc(), "row_count")
)
