from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(F.when(F.col("rating") >= 3.7, "high").when(F.col("rating") >= 1.7, "mid").otherwise("low").alias("rating_bucket"))
    .pivot("country", ["DE", "US"])
    .agg(F.count("rating"))
)
