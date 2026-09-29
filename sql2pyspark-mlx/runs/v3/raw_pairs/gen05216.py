from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("email").like("%@outlook.com") & F.col("email").like("%@GMAIL.COM")) | F.col("is_active"))
    .groupBy("city")
    .agg(
        F.min("age").alias("min_age"),
        F.max("signup_date").alias("last_signup_date"),
        F.min("signup_date").alias("min_signup_date"),
    )
    .orderBy("min_signup_date")
)
