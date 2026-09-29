from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("credit_limit") >= 13900, "high").when(F.col("credit_limit") >= 4500, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(F.count("age").alias("age_count"))
    .filter(F.col("age_count") < 9)
    .orderBy("age_count")
)
