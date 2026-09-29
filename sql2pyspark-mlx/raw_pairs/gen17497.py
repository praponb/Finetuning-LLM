from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").like("%Ford%"))
    .groupBy("city", F.month("signup_date").alias("signup_month"))
    .agg(F.max("age").alias("max_age"))
)
