from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.lower("email").like("%ack%") & (F.col("country") == "SG"))
    .groupBy("country")
    .agg(F.avg("credit_limit").alias("avg_credit_limit"))
    .filter(F.col("avg_credit_limit") < 5300)
    .orderBy(F.col("country").desc(), "avg_credit_limit")
)
