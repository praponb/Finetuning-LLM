from pyspark.sql import functions as F

valid_products = (
    df_products
    .filter(F.col("supplier_id").isin([13, 24, 29]) & (F.col("launch_date") <= "2024-12-28"))
    .withColumn("price_filled", F.coalesce("price", F.lit(0)))
)

products_stats = (
    valid_products
    .groupBy("brand")
    .agg(
        F.min("launch_date").alias("min_launch_date"),
        F.count(F.when(F.col("category") == "Books", 1)).alias("books_count"),
    )
)

result = (
    products_stats
    .select("brand", "books_count")
    .orderBy("brand")
    .limit(3)
)
