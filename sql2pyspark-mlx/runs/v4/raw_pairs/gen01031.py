from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("is_active")
    .agg(
        F.sum(F.when(F.col("segment") == "SMB", F.col("credit_limit")).otherwise(0)).alias("smb_credit_limit"),
        F.count("age").alias("age_count"),
    )
    .filter(F.col("smb_credit_limit") < 52)
    .select("is_active", "smb_credit_limit", "age_count")
    .orderBy("age_count")
)
