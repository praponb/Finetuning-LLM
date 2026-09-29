from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.year("signup_date").alias("signup_year"))
    .agg(
        F.min("signup_date").alias("first_signup_date"),
        F.avg("age").alias("avg_age"),
    )
    .filter(F.col("avg_age") >= 53)
    .select("signup_year", "first_signup_date", "avg_age")
)
