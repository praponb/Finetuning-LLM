from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") < 7100)
    .groupBy("segment", F.month("signup_date").alias("signup_month"))
    .agg(
        F.sum(F.when(F.col("segment") == "Consumer", F.col("credit_limit")).otherwise(0)).alias("consumer_credit_limit"),
        F.count(F.when(F.col("segment") == "SMB", 1)).alias("smb_count"),
    )
    .filter(F.col("smb_count") < 2)
    .drop("smb_count")
)
