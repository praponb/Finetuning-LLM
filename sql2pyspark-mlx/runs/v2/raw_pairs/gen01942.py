from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .groupBy(
        F.when(F.col("s.rating") >= 3.4, "high").when(F.col("s.rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum(F.col("p.cost")).alias("total_cost"),
        F.count(F.when(F.col("p.category") == "Books", 1)).alias("books_count"),
    )
)
