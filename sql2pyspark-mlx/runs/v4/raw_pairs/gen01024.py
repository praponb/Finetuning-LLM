from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("age") >= 58, "high").when(F.col("age") >= 27, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(F.sum("credit_limit").alias("total_credit_limit"))
    .orderBy(F.col("total_credit_limit").desc())
)
