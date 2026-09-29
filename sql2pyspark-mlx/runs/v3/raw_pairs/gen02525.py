from pyspark.sql import functions as F

src = (
    df_customers
    .filter((F.length("email") < 12) & (F.col("signup_date") >= "2023-08-01"))
)

country_summary = (
    src
    .groupBy("country")
    .agg(
        F.min("age").alias("min_age"),
        F.count("credit_limit").alias("credit_limit_count"),
    )
)

result = (
    country_summary
    .filter(F.col("credit_limit_count") < 4)
    .select("country", "credit_limit_count")
)
