from pyspark.sql import functions as F

src = (
    df_customers
    .filter(F.col("age").isNotNull() & F.col("is_active"))
    .withColumn("signup_date_plus_months", F.add_months("signup_date", 6))
)

summary = (
    src
    .groupBy("city", F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .agg(
        F.min("signup_date").alias("first_signup_date"),
        (F.max("age") - F.min("age")).alias("age_range"),
    )
)

result = (
    summary
    .filter(F.col("age_range") < 71)
    .orderBy("signup_month")
)
