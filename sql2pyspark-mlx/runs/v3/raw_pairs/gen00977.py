from pyspark.sql import functions as F

recent_customers = (
    df_customers
    .filter(F.col("signup_date") <= "2024-05-01")
    .withColumn("email_lower", F.lower("email"))
)

summary = (
    recent_customers
    .groupBy("city")
    .agg(F.max("credit_limit").alias("max_credit_limit"))
)

result = summary.filter(F.col("max_credit_limit") < 8200)
