from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") == "London")
    .groupBy(
        "country",
        F.when(F.col("city").isin(["Tokyo", "Berlin"]), "group_a").otherwise("other").alias("city_group"),
        F.when(F.col("rating") >= 3.9, "high").when(F.col("rating") >= 1.7, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("city") == "Berlin", F.col("rating")).otherwise(0)).alias("berlin_rating"),
        F.avg("rating").alias("avg_rating"),
    )
    .filter(F.col("avg_rating") >= 3.2)
    .drop("avg_rating")
    .orderBy(F.col("city_group").desc(), "country", "rating_bucket")
    .limit(5)
)
