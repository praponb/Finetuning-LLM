from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit").between(10800, 13400) | (F.col("country") == "TH"))
    .groupBy("segment")
    .agg(F.sum("credit_limit").alias("total_credit_limit"))
    .filter(F.col("total_credit_limit") > 54000)
)
