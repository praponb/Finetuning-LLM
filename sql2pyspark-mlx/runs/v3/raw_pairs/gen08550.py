from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").like("%@GMAIL.COM") & F.col("email").like("%@company.co") & F.col("is_active"))
    .groupBy(
        F.when(F.year("signup_date") < 2024, "before_2024").when(F.year("signup_date") == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
    )
    .agg(
        (F.max("age") - F.min("age")).alias("age_range"),
        F.avg("credit_limit").alias("avg_credit_limit"),
    )
)
