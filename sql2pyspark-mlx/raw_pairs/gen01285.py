from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("%Yoga%"))
    .groupBy(
        "supplier_id",
        F.when(F.col("cost") >= 560, "high").when(F.col("cost") >= 270, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("category") == "Books", F.col("price")).otherwise(0)).alias("books_price"),
        (F.max("price") - F.min("price")).alias("price_range"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") >= 5)
    .drop("cnt")
    .orderBy(F.col("price_range").desc())
)
