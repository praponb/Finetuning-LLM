from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "TH")
    .groupBy(F.when(F.col("rating") >= 3.7, "high").when(F.col("rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"))
    .pivot("country", ["TH", "DE"])
    .agg(F.sum("rating"))
    .orderBy("rating_bucket")
)
