from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") > 10800)
    .groupBy("is_active")
    .agg(F.avg("credit_limit").alias("avg_credit_limit"))
    .filter(F.col("avg_credit_limit") >= 9100)
    .orderBy("avg_credit_limit")
)
