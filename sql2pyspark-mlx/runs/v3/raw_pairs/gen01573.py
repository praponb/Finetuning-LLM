from pyspark.sql import functions as F

base = (
    df_products
    .filter(~((F.col("launch_date") >= "2025-01-01") & F.col("product_name").like("D%") & F.col("product_name").like("wi%")))
)

summary = (
    base
    .groupBy("category")
    .agg(
        F.count(F.when(F.col("category") == "Books", 1)).alias("books_count"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
    )
)

result = (
    summary
    .filter(F.col("books_count") >= 5)
    .orderBy(F.col("books_count").desc(), "category")
    .limit(3)
)
