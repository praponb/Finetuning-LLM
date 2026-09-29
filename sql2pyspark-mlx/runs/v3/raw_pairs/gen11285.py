from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("credit_limit") <= 15900) & F.col("credit_limit").between(4800, 9100))
    .rollup("is_active", F.year("signup_date").alias("signup_year"))
    .agg(F.max("age").alias("max_age"))
)
