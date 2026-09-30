from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("is_active")
    .agg(
        F.min("age").alias("min_age"),
        F.min("signup_date").alias("first_signup_date"),
    )
    .orderBy("first_signup_date", "min_age")
)
