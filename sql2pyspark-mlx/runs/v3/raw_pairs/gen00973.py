from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 3.6, "high").when(F.col("rating") >= 2.1, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(
        F.min("rating").alias("min_rating"),
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_count"),
    )
    .orderBy("city")
)
