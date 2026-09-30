from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("city") == "London") & (F.col("rating") > 3.8))
    .groupBy(
        F.when(F.col("rating") >= 3.5, "high").when(F.col("rating") >= 1.5, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.count(F.when(F.col("country") == "SG", 1)).alias("sg_count"),
        F.count("*").alias("row_count"),
    )
    .orderBy("rating_bucket")
    .limit(5)
)
