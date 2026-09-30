from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.when(F.col("price") >= 930, "high").when(F.col("price") >= 310, "mid").otherwise("low").alias("price_bucket"),
    )
    .agg(
        F.min("cost").alias("min_cost"),
        F.round(((100 * F.sum(F.when(F.col("category") == "Books", 1).otherwise(0))) / F.count("*")), 1).alias("pct_books"),
    )
)
