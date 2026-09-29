from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Wholesale%") | (F.col("city") == "Singapore"))
)

summary = (
    base
    .groupBy(
        F.when(F.col("rating") >= 4.6, "high").when(F.col("rating") >= 1.7, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.min("rating").alias("min_rating"))
)

result = (
    summary
    .filter(F.col("min_rating") >= 2.7)
    .select("rating_bucket", "min_rating")
    .orderBy("rating_bucket")
    .limit(5)
)
