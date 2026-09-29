from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.0, "high").when(F.col("rating") >= 2.2, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(
        F.sum("rating").alias("total_rating"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("total_rating") < 24)
    .orderBy(F.col("city").desc())
)
