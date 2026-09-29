from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment")
    .agg(F.sum("credit_limit").alias("total_credit_limit"))
    .orderBy("total_credit_limit")
)
