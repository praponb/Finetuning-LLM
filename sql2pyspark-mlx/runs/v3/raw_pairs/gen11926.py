from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").isNotNull() & (F.col("segment") == "Consumer"))
    .groupBy(
        F.when(F.year("signup_date") < 2024, "before_2024").when(F.year("signup_date") == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
        F.month("signup_date").alias("signup_month"),
    )
    .agg(
        F.sum(F.when(F.col("city") == "New York", F.col("credit_limit")).otherwise(0)).alias("new_york_credit_limit"),
    )
    .orderBy("new_york_credit_limit")
)
