from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "category",
        "price",
        F.when(F.col("category") == "Books", 1).otherwise(0).alias("is_books"),
        F.floor("cost").alias("cost_whole"),
    )
)
