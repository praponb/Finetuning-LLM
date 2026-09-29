from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") > 6700)
    .groupBy(F.year("signup_date").alias("signup_year"))
    .agg(
        F.count("*").alias("customer_count"),
        F.max("signup_date").alias("last_signup_date"),
    )
)
