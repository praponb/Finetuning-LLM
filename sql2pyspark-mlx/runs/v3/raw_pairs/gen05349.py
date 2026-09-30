from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Tok%") | (F.col("country") == "UK"))
    .groupBy(
        F.when(F.col("rating") >= 3.5, "high").when(F.col("rating") >= 2.1, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.avg("rating").alias("avg_rating"))
    .filter(F.col("avg_rating") > 2.3)
)
