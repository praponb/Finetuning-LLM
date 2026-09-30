from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.year("signup_date") < 2024, "before_2024").when(F.year("signup_date") == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
        F.quarter("signup_date").alias("signup_quarter"),
    )
    .agg(F.min("age").alias("min_age"))
    .filter(F.col("min_age") >= 52)
    .select("signup_cohort", "signup_quarter", "min_age")
    .orderBy("signup_cohort")
)
