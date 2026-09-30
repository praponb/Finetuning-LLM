from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.year("signup_date") < 2024, "before_2024").when(F.year("signup_date") == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
        F.quarter("signup_date").alias("signup_quarter"),
    )
    .agg(
        F.min("signup_date").alias("first_signup_date"),
        F.sum("age").alias("total_age"),
        F.count("credit_limit").alias("credit_limit_count"),
    )
)
