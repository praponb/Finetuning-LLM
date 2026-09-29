from pyspark.sql import functions as F

src = df_suppliers.filter(F.col("rating") > 2.6)

summary = (
    src
    .groupBy(
        F.when(F.col("country").isin(["UK", "DE"]), "group_a").otherwise("other").alias("country_group"),
        F.when(F.col("rating") >= 4.4, "high").when(F.col("rating") >= 2.3, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "London", 1).otherwise(0))) / F.count("*")), 1).alias("pct_london"),
        F.avg("rating").alias("avg_rating"),
    )
)

result = (
    summary
    .filter(F.col("avg_rating") < 4.2)
    .orderBy(F.col("pct_london").desc(), "country_group")
)
