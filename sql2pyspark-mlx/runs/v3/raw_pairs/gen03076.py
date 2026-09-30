from pyspark.sql import functions as F

valid_customers = df_customers.filter(F.col("city").isin(["New York", "Bangkok", "Berlin"]))

summary = (
    valid_customers
    .groupBy(
        "is_active",
        F.when(F.year("signup_date") < 2024, "before_2024").when(F.year("signup_date") == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
    )
    .agg(
        F.avg("credit_limit").alias("avg_credit_limit"),
        F.max("signup_date").alias("latest_signup_date"),
    )
)

result = (
    summary
    .filter(F.col("avg_credit_limit") < 9300)
    .select("is_active", "signup_cohort", "avg_credit_limit", "latest_signup_date")
)
