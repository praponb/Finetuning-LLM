from pyspark.sql import functions as F

result = (
    df_products
    .agg(
        F.sum(F.when(F.col("category") == "Books", F.col("cost")).otherwise(0)).alias("books_cost"),
        F.sum("price").alias("total_price"),
    )
    .orderBy(F.col("books_cost").desc())
)
