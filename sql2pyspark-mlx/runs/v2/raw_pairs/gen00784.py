from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") > 3.7)
    .groupBy(
        "country",
        F.when(F.col("rating") >= 3.5, "high").when(F.col("rating") >= 1.8, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.count("*").alias("supplier_count"))
    .filter(F.col("supplier_count") > 1)
)
