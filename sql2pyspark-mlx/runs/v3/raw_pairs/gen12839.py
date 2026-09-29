from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.date_format("signup_date", "yyyy-MM").alias("signup_month"))
    .agg(F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"))
    .orderBy("signup_month")
)
