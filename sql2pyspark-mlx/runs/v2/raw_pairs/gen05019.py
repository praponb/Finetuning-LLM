from pyspark.sql import functions as F

recent_suppliers = df_suppliers.filter(F.col("rating") > 3.0)

agg = (
    recent_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.0, "high").when(F.col("rating") >= 2.1, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(F.round(F.stddev("rating"), 2).alias("stddev_rating"))
)

result = (
    agg
    .filter(F.col("stddev_rating") > 33)
    .orderBy(F.col("rating_bucket").desc(), F.col("stddev_rating").desc(), "city")
    .limit(5)
)
