from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("is_active") & (F.col("country") == "TH"))
    .rollup("segment", F.quarter("signup_date").alias("signup_quarter"))
    .agg(F.sum("age").alias("total_age"))
    .orderBy(F.col("signup_quarter").desc())
)
