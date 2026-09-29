from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("segment") == "Corporate") & ((F.col("country") == "TH") | (F.col("signup_date") >= "2023-07-01")))
    .groupBy(F.month("signup_date").alias("signup_month"))
    .agg(F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"))
    .filter(F.col("avg_credit_limit") >= 76)
    .orderBy(F.col("avg_credit_limit").desc())
)
