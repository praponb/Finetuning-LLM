from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("city").isin(["Chiang Mai", "Berlin", "Tokyo"]) & F.col("country").isin(["SG", "TH", "DE"]))
    .groupBy(
        F.when(F.col("rating") >= 4.4, "high").when(F.col("rating") >= 1.4, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.min("rating").alias("min_rating"))
)
