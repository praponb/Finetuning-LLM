from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.quarter("signup_date").alias("signup_quarter"), "country")
    .agg(F.sum("credit_limit").alias("total_credit_limit"))
    .orderBy(F.col("country").desc())
)
