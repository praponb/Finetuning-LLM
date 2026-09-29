from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("credit_limit") >= 12900, "high").when(F.col("credit_limit") >= 3800, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(F.min("age").alias("min_age"))
)
