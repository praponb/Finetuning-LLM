from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("credit_limit") >= 12700, "high").when(F.col("credit_limit") >= 5400, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(F.round(F.avg("age"), 2).alias("avg_age"))
    .filter(F.col("avg_age") >= 32)
    .orderBy(F.col("avg_age").desc())
)
