from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment").isNotNull())
    .groupBy(
        F.quarter("signup_date").alias("signup_quarter"),
        F.when(F.col("age") >= 68, "high").when(F.col("age") >= 40, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(
        F.max("signup_date").alias("last_signup_date"),
        F.avg("age").alias("avg_age"),
    )
)
