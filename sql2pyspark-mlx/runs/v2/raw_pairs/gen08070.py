from pyspark.sql import functions as F

src = (
    df_suppliers
    .filter(F.col("rating").between(3.2, 4.1) | ~F.col("country").isin(["UK", "JP"]))
    .withColumn("country_upper", F.upper("country"))
)

summary = (
    src
    .groupBy(
        F.when(F.col("rating") >= 3.9, "high").when(F.col("rating") >= 2.5, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.countDistinct("rating").alias("unique_countrys"),
        F.avg("rating").alias("avg_rating"),
    )
)

result = (
    summary
    .select("rating_bucket", "avg_rating")
    .orderBy(F.col("avg_rating").desc())
)
