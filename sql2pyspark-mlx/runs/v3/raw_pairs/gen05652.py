from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.quarter("signup_date").alias("signup_quarter"))
    .pivot("segment", ["SMB", "Consumer"])
    .agg(F.avg("credit_limit"))
)
