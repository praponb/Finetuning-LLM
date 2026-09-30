from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active"))
    .groupBy(F.quarter("signup_date").alias("signup_quarter"))
    .agg(F.count(F.when(F.col("segment") == "SMB", 1)).alias("smb_count"))
    .orderBy(F.col("smb_count").desc(), F.col("signup_quarter").desc())
    .limit(50)
)
