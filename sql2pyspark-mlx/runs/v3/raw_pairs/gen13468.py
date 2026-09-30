from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(F.when(F.col("rating") >= 3.8, "high").when(F.col("rating") >= 2.4, "mid").otherwise("low").alias("rating_bucket"))
    .pivot("country", ["UK", "DE", "TH", "US"])
    .agg(F.count("rating"))
    .orderBy("rating_bucket")
)
