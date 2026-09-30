from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("credit_limit") >= 14700, "high").when(F.col("credit_limit") >= 5100, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(F.round(F.avg("age"), 2).alias("avg_age"))
    .filter(F.col("avg_age") >= 69)
    .select("credit_limit_bucket", "avg_age")
)
