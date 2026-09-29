from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit").between(4900, 6300))
    .groupBy(F.date_format("signup_date", "yyyy-MM").alias("signup_month"), "segment")
    .agg(F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"))
    .filter(F.col("avg_credit_limit") < 63)
    .orderBy(F.col("signup_month").desc())
)
