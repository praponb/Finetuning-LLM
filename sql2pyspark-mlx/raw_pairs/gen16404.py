from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("supplier_name").like("%Globa%"))
    .groupBy(
        "country",
        F.when(F.col("rating") >= 3.7, "high").when(F.col("rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.sum("rating").alias("total_rating"))
    .filter(F.col("total_rating") >= 24)
    .select("country", "rating_bucket", "total_rating")
    .orderBy(F.col("total_rating").desc(), "country")
)
