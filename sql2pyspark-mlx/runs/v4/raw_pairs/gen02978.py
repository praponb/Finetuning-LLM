from pyspark.sql import functions as F

src = df_products.filter((F.col("supplier_id") == 2) | (F.year("launch_date") == 2025))

products_stats = (
    src
    .groupBy("category")
    .agg(
        F.sum(F.when(F.col("category") == "Books", 1).otherwise(0)).alias("books_count"),
    )
)

result = products_stats.select("category", "books_count")
