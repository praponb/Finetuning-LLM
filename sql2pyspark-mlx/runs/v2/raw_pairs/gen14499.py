from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.length("supplier_name") > 12)
    .groupBy(
        F.when(F.col("rating") >= 4.1, "high").when(F.col("rating") >= 1.6, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.count(F.when(F.col("country") == "UK", 1)).alias("uk_count"))
    .orderBy(F.col("uk_count").desc())
)
