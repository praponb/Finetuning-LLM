from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("is_active")
    .agg(
        F.sum("credit_limit").alias("total_credit_limit"),
        F.sum(F.when(F.col("segment") == "SMB", 1).otherwise(0)).alias("smb_count"),
    )
    .filter(F.col("smb_count") < 29)
)
