from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("city").isin(["Bangkok", "Tokyo", "London"]) & (F.col("rating") >= 1.9)) | (F.col("country") == "TH"))
    .groupBy("country", "city")
    .agg(
        F.countDistinct("rating").alias("unique_countrys"),
        F.avg("rating").alias("avg_rating"),
    )
    .orderBy("city")
)
