from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand").isin(["Umbrella", "Acme"]))
    .groupBy("category")
    .agg(
        F.sum(F.when(F.col("category") == "Books", 1).otherwise(0)).alias("books_count"),
    )
    .filter(F.col("books_count") < 48)
    .orderBy("books_count")
)
