from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("is_active")
    .agg(F.avg("credit_limit").alias("avg_credit_limit"))
    .filter(F.col("avg_credit_limit") > 4500)
    .orderBy(F.col("avg_credit_limit").desc())
)
