from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(F.when(F.col("rating") >= 3.6, "high").when(F.col("rating") >= 2.3, "mid").otherwise("low").alias("rating_bucket"))
    .pivot("country", ["JP", "UK", "US", "TH"])
    .agg(F.sum("rating"))
)
