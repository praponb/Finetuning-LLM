from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("credit_limit") >= 12700, "high").when(F.col("credit_limit") >= 7100, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("segment") == "SMB", F.col("credit_limit")).otherwise(0)).alias("smb_credit_limit"),
    )
    .orderBy("smb_credit_limit")
)
