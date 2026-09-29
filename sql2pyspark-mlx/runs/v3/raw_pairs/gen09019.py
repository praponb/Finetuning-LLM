from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active"))
    .groupBy("segment")
    .agg(F.count(F.when(F.col("segment") == "SMB", 1)).alias("smb_count"))
)
