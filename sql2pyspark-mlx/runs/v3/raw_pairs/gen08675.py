from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(F.when(F.col("rating") >= 4.4, "high").when(F.col("rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"))
    .pivot("city", ["Berlin", "Tokyo", "New York", "London"])
    .agg(F.sum("rating"))
)
