from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(F.when(F.col("rating") >= 4.2, "high").when(F.col("rating") >= 1.5, "mid").otherwise("low").alias("rating_bucket"))
    .pivot("city", ["London", "Singapore"])
    .agg(F.max("rating"))
)
