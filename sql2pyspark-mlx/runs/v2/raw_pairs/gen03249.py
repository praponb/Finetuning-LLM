from pyspark.sql import functions as F

filtered_suppliers = df_suppliers.filter(F.col("rating") <= 2.8)

summary = (
    filtered_suppliers
    .groupBy(
        "city",
        F.when(F.col("rating") >= 4.0, "high").when(F.col("rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.round(F.stddev("rating"), 2).alias("stddev_rating"))
)

result = (
    summary
    .orderBy(F.col("stddev_rating").desc(), "city", "rating_bucket")
    .limit(20)
)
