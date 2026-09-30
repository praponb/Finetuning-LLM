from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment").isNotNull() & ((F.col("credit_limit") < 6800) | F.col("city").isNotNull()))
    .withColumn("signup_date_period", F.date_trunc("MONTH", "signup_date"))
)
