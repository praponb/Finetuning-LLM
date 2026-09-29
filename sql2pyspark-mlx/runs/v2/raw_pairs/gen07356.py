from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country", "segment")
    .agg(F.min("credit_limit").alias("min_credit_limit"))
)
