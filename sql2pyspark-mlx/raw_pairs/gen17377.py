from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(
        F.min("price").alias("min_price"),
        F.round(((100 * F.sum(F.when(F.col("brand") == "Acme", 1).otherwise(0))) / F.count("*")), 1).alias("pct_acme"),
        F.sum(F.when(F.col("category") == "Books", 1).otherwise(0)).alias("books_count"),
    )
    .orderBy("books_count", "brand")
    .limit(50)
)
