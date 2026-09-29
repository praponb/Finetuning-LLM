from pyspark.sql import functions as F

filtered_customers = df_customers.filter(F.col("segment") != "Corporate")

signup_cohort_summary = (
    filtered_customers
    .groupBy(
        F.when(F.year("signup_date") < 2024, "before_2024").when(F.year("signup_date") == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
    )
    .agg(F.round(F.stddev("credit_limit"), 2).alias("stddev_credit_limit"))
)

result = (
    signup_cohort_summary
    .filter(F.col("stddev_credit_limit") > 55)
    .select("signup_cohort", "stddev_credit_limit")
    .orderBy("signup_cohort")
)
