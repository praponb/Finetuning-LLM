from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit").between(8600, 10200) & (F.col("age") != 66) & F.col("is_active"))
    .groupBy("country")
    .agg(
        F.min("signup_date").alias("first_signup_date"),
        F.sum("age").alias("total_age"),
    )
    .orderBy(F.col("country").desc(), "first_signup_date")
)
