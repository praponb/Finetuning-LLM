from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country")
    .agg(F.max("credit_limit").alias("max_credit_limit"))
    .filter(F.col("max_credit_limit") >= 10400)
)
