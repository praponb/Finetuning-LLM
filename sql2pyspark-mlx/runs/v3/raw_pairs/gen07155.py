from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("age") >= 62, "high").when(F.col("age") >= 30, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(F.count("credit_limit").alias("credit_limit_count"))
    .filter(F.col("credit_limit_count") < 5)
)
