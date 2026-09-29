from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.5, "high").when(F.col("rating") >= 1.8, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.count("rating").alias("rating_count"))
)
