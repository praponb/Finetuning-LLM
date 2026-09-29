from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("country").isin(["JP", "DE"]), "group_a").otherwise("other").alias("country_group"),
        F.when(F.col("rating") >= 4.4, "high").when(F.col("rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.min("rating").alias("min_rating"),
        F.sum(F.when(F.col("country") == "US", F.col("rating")).otherwise(0)).alias("us_rating"),
    )
    .filter(F.col("us_rating") > 25)
    .select("country_group", "rating_bucket", "min_rating", "us_rating")
)
