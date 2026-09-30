from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        "country",
        F.when(F.col("credit_limit") >= 16300, "high").when(F.col("credit_limit") >= 6400, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(F.countDistinct("age").alias("unique_segments"))
    .filter(F.col("unique_segments") < 4)
    .select("country", "credit_limit_bucket", "unique_segments")
    .orderBy(F.col("unique_segments").desc())
)
