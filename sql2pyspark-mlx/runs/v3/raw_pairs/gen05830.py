from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 3.6, "high").when(F.col("rating") >= 2.2, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.sum("rating").alias("total_rating"))
    .filter(F.col("total_rating") >= 9)
)
