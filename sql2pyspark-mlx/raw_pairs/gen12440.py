from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.date_format("signup_date", "yyyy-MM").alias("signup_month"))
    .agg(F.count(F.when(F.col("segment") == "SMB", 1)).alias("smb_count"))
    .filter(F.col("smb_count") >= 3)
)
