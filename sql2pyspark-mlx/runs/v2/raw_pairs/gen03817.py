from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit").isNull() & F.col("email").isNotNull())
    .select(
        "city",
        "signup_date",
        F.ceil("age").alias("age_whole"),
        F.lower("city").alias("city_lower"),
    )
)
