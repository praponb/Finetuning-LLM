from pyspark.sql import functions as F

result = (
    df_products
    .agg(
        F.sum(F.when(F.col("category") == "Books", F.col("price")).otherwise(0)).alias("books_price"),
    )
    .orderBy(F.col("books_price").desc())
)
