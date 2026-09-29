from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("city") == "Bangkok") & F.col("rating").between(1.8, 3.2))
    .groupBy(
        F.when(F.col("rating") >= 4.3, "high").when(F.col("rating") >= 2.1, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("country") == "SG", F.col("rating")).otherwise(0)).alias("sg_rating"),
        F.max("rating").alias("max_rating"),
    )
    .filter(F.col("max_rating") >= 3.4)
)
