from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age").between(57, 67))
    .groupBy(
        "city",
        F.when(F.year("signup_date") < 2024, "before_2024").when(F.year("signup_date") == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
    )
    .agg(F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"))
    .filter(F.col("avg_credit_limit") > 73)
    .select("city", "signup_cohort", "avg_credit_limit")
)
