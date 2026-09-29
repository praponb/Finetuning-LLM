from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") >= 3.0) & ((F.col("country") == "TH") | F.col("country").isin(["SG", "TH", "JP"])))
    .groupBy(
        F.when(F.col("rating") >= 4.6, "high").when(F.col("rating") >= 1.8, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.count(F.when(F.col("city") == "London", 1)).alias("london_count"),
    )
    .orderBy(F.col("cnt").desc(), "rating_bucket")
    .limit(50)
)
