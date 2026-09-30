from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.quarter("signup_date").alias("signup_quarter"))
    .agg(F.count("age").alias("age_count"))
    .filter(F.col("age_count") < 7)
    .select("signup_quarter", "age_count")
)
