from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.month("signup_date").isin([3, 7, 8]))
    .groupBy(
        F.date_trunc("MONTH", "signup_date").alias("signup_month"),
        F.when(F.year("signup_date") < 2024, "before_2024").when(F.year("signup_date") == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
    )
    .agg(
        F.sum(F.when(F.col("country") == "US", F.col("credit_limit")).otherwise(0)).alias("us_credit_limit"),
    )
)
