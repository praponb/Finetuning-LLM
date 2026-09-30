from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(
        F.min("launch_date").alias("min_launch_date"),
        F.sum(F.when(F.col("category") == "Books", 1).otherwise(0)).alias("books_count"),
        F.sum("cost").alias("total_cost"),
    )
)
