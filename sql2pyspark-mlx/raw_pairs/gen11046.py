from pyspark.sql import functions as F

valid_customers = df_customers.filter((F.col("age") > 43) & F.upper("email").like("%RAC%"))

summary = (
    valid_customers
    .groupBy(
        F.when(F.year("signup_date") < 2024, "before_2024").when(F.year("signup_date") == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
    )
    .agg(
        F.sum(F.when(F.col("city") == "Bangkok", 1).otherwise(0)).alias("bangkok_count"),
    )
)

result = summary.select("signup_cohort", "bangkok_count")
