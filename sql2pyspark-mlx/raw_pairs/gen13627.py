from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.year("signup_date") >= 2025)
    .groupBy(
        F.when(F.year("signup_date") < 2024, "before_2024").when(F.year("signup_date") == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
        F.year("signup_date").alias("signup_year"),
    )
    .agg(F.max("age").alias("max_age"))
    .filter(F.col("max_age") < 39)
    .orderBy("signup_year")
)
