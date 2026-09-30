from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") == "Berlin")
    .groupBy(
        F.when(F.col("rating") >= 4.5, "high").when(F.col("rating") >= 1.9, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.count("*").alias("row_count"))
    .filter(F.col("row_count") >= 1)
    .orderBy("rating_bucket")
    .limit(50)
)
