from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") > 33)
    .groupBy(
        F.when(F.year("signup_date") < 2024, "before_2024").when(F.year("signup_date") == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
        F.when(F.col("credit_limit") >= 16500, "high").when(F.col("credit_limit") >= 7600, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(
        F.min("age").alias("min_age"),
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
        F.max("signup_date").alias("latest_signup_date"),
    )
)
