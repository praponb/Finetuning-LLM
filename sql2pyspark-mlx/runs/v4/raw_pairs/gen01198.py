from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~((F.col("country") == "SG") & (F.col("signup_date") > "2023-11-01")))
    .groupBy(F.quarter("signup_date").alias("signup_quarter"))
    .agg(F.min("signup_date").alias("min_signup_date"))
)
