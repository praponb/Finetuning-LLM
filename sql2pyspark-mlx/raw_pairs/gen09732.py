from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.year("signup_date").alias("signup_year"))
    .agg(
        F.count("age").alias("age_count"),
        F.min("credit_limit").alias("min_credit_limit"),
    )
    .orderBy(F.col("min_credit_limit").desc(), F.col("age_count").desc(), "signup_year")
    .limit(10)
)
