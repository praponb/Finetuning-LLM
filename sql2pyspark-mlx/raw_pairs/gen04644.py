from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("credit_limit") >= 16700, "high").when(F.col("credit_limit") >= 8300, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(F.count("*").alias("cnt"))
    .filter(F.col("cnt") >= 7)
)
