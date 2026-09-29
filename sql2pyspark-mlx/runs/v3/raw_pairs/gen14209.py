from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter((F.col("rating") > 2.9) & (F.col("rating") < 1.9))
    .withColumn("supplier_name_prefix", F.substring("supplier_name", 1, 2))
)

summary = (
    base
    .groupBy(
        F.when(F.col("rating") >= 3.7, "high").when(F.col("rating") >= 1.5, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum("rating").alias("total_rating"),
        F.count(F.when(F.col("city") == "Bangkok", 1)).alias("bangkok_count"),
    )
)

result = (
    summary
    .filter(F.col("total_rating") < 12)
    .select("rating_bucket", "total_rating", "bangkok_count")
    .orderBy("total_rating")
)
