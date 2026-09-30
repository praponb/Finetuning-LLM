from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").isNotNull())
    .groupBy(
        "is_active",
        F.when(F.year("signup_date") < 2024, "before_2024").when(F.year("signup_date") == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
    )
    .agg(
        F.min("credit_limit").alias("min_credit_limit"),
        F.sum("credit_limit").alias("total_credit_limit"),
    )
    .filter(F.col("min_credit_limit") >= 14400)
    .orderBy(F.col("min_credit_limit").desc())
)
