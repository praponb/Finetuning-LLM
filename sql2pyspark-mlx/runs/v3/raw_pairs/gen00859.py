from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.year("signup_date").alias("signup_year"))
    .agg(F.sum("credit_limit").alias("total_credit_limit"))
    .orderBy(F.col("total_credit_limit").desc())
)
