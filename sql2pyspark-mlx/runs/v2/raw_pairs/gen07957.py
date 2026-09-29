from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["Bangkok", "Berlin", "Chiang Mai"]))
    .groupBy(
        F.when(F.col("rating") >= 4.4, "high").when(F.col("rating") >= 1.7, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_count"))
    .orderBy(F.col("chiang_mai_count").desc())
)
