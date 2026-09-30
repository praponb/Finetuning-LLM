from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.year("launch_date") == 2024)
    .groupBy("category")
    .agg(
        F.sum(F.when(F.col("category") == "Books", F.col("price")).otherwise(0)).alias("books_price"),
        F.count(F.when(F.col("brand") == "Globex", 1)).alias("globex_count"),
    )
    .filter(F.col("globex_count") >= 5)
    .drop("globex_count")
)
