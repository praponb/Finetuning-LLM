from pyspark.sql import functions as F

filtered_suppliers = df_suppliers.filter(F.col("city") != "London")

suppliers_stats = (
    filtered_suppliers
    .groupBy(
        F.when(F.col("rating") >= 3.4, "high").when(F.col("rating") >= 2.1, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.min("rating").alias("min_rating"))
)

result = (
    suppliers_stats
    .select("rating_bucket", "min_rating")
    .orderBy(F.col("rating_bucket").desc(), F.col("min_rating").desc())
)
