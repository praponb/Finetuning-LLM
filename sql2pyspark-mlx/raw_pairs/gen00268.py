from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").isNull() | F.col("is_active"))
    .agg(F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"))
    .orderBy("avg_credit_limit")
)
