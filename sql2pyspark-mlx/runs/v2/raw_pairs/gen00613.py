from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active") & F.col("age").between(53, 56) & (F.col("age") == 53))
    .groupBy("city", F.month("signup_date").alias("signup_month"))
    .agg(F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"))
    .filter(F.col("avg_credit_limit") >= 69)
    .orderBy("city", F.col("signup_month").desc())
    .limit(50)
)
