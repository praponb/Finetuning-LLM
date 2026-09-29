from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment")
    .agg(F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"))
    .filter(F.col("avg_credit_limit") >= 70)
    .orderBy("avg_credit_limit", F.col("segment").desc())
    .limit(20)
)
