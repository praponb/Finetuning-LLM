from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") > 3.2)
    .groupBy(F.when(F.col("rating") >= 4.5, "high").when(F.col("rating") >= 1.9, "mid").otherwise("low").alias("rating_bucket"))
    .pivot("country", ["UK", "JP"])
    .agg(F.max("rating"))
)
