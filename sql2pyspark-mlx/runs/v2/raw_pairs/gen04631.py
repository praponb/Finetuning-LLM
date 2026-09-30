from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.month("signup_date").alias("signup_month"))
    .agg(
        F.max("signup_date").alias("last_signup_date"),
        F.min("age").alias("min_age"),
        F.avg("age").alias("avg_age"),
    )
    .orderBy("min_age")
)
