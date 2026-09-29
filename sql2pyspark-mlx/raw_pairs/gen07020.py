from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull())
    .groupBy(
        F.when(F.col("rating") >= 3.9, "high").when(F.col("rating") >= 2.2, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.max("rating").alias("max_rating"),
        F.count(F.when(F.col("country") == "US", 1)).alias("us_count"),
    )
)
