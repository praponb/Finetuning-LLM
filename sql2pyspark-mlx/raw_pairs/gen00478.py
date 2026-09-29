from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age").between(53, 54))
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .pivot("city", ["Tokyo", "Bangkok", "Chiang Mai"])
    .agg(F.max("credit_limit"))
    .orderBy("signup_month")
)
