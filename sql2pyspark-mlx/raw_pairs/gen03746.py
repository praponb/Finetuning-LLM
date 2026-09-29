from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(F.when(F.col("rating") >= 4.3, "high").when(F.col("rating") >= 1.9, "mid").otherwise("low").alias("rating_bucket"))
    .pivot("city", ["Singapore", "Berlin", "London"])
    .agg(F.sum("rating"))
    .orderBy("rating_bucket")
)
