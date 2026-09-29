from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("credit_limit") <= 11900) | (F.col("age") < 58))
    .groupBy(
        F.when(F.col("age") >= 66, "high").when(F.col("age") >= 38, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.avg("credit_limit").alias("avg_credit_limit"),
    )
    .orderBy(F.col("age_bucket").desc())
)
