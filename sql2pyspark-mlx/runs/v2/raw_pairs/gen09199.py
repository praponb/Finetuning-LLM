from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.month("signup_date").isin([2, 3, 7]))
    .groupBy("is_active", F.month("signup_date").alias("signup_month"))
    .agg(F.avg("credit_limit").alias("avg_credit_limit"))
    .orderBy(F.col("signup_month").desc(), "is_active")
    .limit(50)
)
