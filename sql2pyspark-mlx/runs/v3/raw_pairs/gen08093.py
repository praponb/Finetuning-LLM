from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.year("signup_date") == 2025)
    .withColumn("credit_limit_plus_age", (F.col("credit_limit") + F.col("age")))
    .orderBy("segment")
)
