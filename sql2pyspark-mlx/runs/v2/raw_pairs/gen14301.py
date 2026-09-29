from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost").between(500, 590))
    .groupBy(F.year("launch_date").alias("launch_year"))
    .agg(
        F.count(F.when(F.col("category") == "Books", 1)).alias("books_count"),
        F.round(((100 * F.sum(F.when(F.col("category") == "Home", 1).otherwise(0))) / F.count("*")), 1).alias("pct_home"),
    )
    .filter(F.col("pct_home") < 42)
    .select("launch_year", "books_count", "pct_home")
    .orderBy(F.col("books_count").desc(), F.col("pct_home").desc())
)
