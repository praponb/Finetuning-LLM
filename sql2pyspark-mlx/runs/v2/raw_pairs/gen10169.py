from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter((F.col("country") == "DE") & ((F.col("city") != "London") | F.col("supplier_name").like("%Suppl%")))
)

summary = (
    base
    .groupBy(
        F.when(F.col("rating") >= 4.5, "high").when(F.col("rating") >= 2.4, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
        F.sum(F.when(F.col("country") == "JP", F.col("rating")).otherwise(0)).alias("jp_rating"),
    )
)

result = summary.orderBy(F.col("jp_rating").desc())
