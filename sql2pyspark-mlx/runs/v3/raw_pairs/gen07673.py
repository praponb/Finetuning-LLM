from pyspark.sql import functions as F

result = (
    df_products
    .agg(
        F.sum(F.when(F.col("category") == "Books", 1).otherwise(0)).alias("books_count"),
        F.count("stock_qty").alias("stock_qty_count"),
    )
    .orderBy(F.col("stock_qty_count").desc(), F.col("books_count").desc())
)
