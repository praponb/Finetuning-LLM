from pyspark.sql import functions as F

result = (
    df_products
    .agg(
        F.sum("cost").alias("total_cost"),
        F.count(F.when(F.col("category") == "Books", 1)).alias("books_count"),
    )
    .orderBy(F.col("total_cost").desc(), F.col("books_count").desc())
)
