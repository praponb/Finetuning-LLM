from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city") != "Chiang Mai")
    .groupBy(F.date_format("signup_date", "yyyy-MM").alias("signup_month"))
    .agg(F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"))
    .filter(F.col("avg_credit_limit") >= 21)
    .select("signup_month", "avg_credit_limit")
)
