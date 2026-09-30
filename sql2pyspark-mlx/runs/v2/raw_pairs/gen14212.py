from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(F.when(F.col("rating") >= 3.6, "high").when(F.col("rating") >= 2.2, "mid").otherwise("low").alias("rating_bucket"))
    .pivot("city", ["Berlin", "London"])
    .agg(F.sum("rating"))
    .orderBy("rating_bucket")
)
