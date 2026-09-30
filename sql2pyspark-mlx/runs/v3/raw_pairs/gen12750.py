from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("age") >= 71, "high").when(F.col("age") >= 41, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
        F.max("signup_date").alias("last_signup_date"),
    )
    .filter(F.col("avg_credit_limit") > 23)
)
