from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("supplier_id") == 40) | F.col("brand").isNull())
    .groupBy(
        "category",
        F.when(F.col("cost") >= 660, "high").when(F.col("cost") >= 190, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.sum(F.when(F.col("category") == "Books", F.col("price")).otherwise(0)).alias("books_price"),
    )
    .orderBy(F.col("cost_range").desc())
)
