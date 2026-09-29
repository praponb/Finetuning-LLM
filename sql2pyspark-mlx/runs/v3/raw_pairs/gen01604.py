from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") == 49)
    .groupBy(
        F.when(F.col("age") >= 58, "high").when(F.col("age") >= 41, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(F.countDistinct("age").alias("unique_segments"))
    .filter(F.col("unique_segments") >= 1)
    .orderBy("unique_segments", "age_bucket")
)
