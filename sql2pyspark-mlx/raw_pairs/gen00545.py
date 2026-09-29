from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").like("Kan%"))
    .groupBy("is_active")
    .agg(
        F.min("age").alias("min_age"),
        F.min("signup_date").alias("first_signup_date"),
    )
    .filter(F.col("min_age") < 57)
)
