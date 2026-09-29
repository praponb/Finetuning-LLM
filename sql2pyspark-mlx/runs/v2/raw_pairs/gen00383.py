from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") <= 14500)
    .groupBy(F.month("signup_date").alias("signup_month"))
    .agg((F.max("credit_limit") - F.min("credit_limit")).alias("credit_limit_range"))
    .filter(F.col("credit_limit_range") < 33)
    .orderBy(F.col("credit_limit_range").desc())
)
