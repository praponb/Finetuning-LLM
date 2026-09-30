from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.quarter("signup_date").alias("signup_quarter"), "country")
    .agg((F.max("age") - F.min("age")).alias("age_range"))
)
