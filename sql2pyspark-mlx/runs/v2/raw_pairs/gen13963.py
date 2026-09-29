from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.6, "high").when(F.col("rating") >= 2.5, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.max("rating").alias("max_rating"))
    .filter(F.col("max_rating") < 2.8)
    .orderBy("rating_bucket")
)
