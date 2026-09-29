from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand", "category")
    .agg(
        F.count(F.when(F.col("category") == "Books", 1)).alias("books_count"),
        F.max("cost").alias("max_cost"),
    )
    .filter(F.col("max_cost") >= 510)
)
