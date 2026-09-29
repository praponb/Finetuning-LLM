from pyspark.sql import functions as F

result = (
    df_customers
    .cube("country", "segment")
    .agg(
        F.max("signup_date").alias("latest_signup_date"),
        F.sum(F.when(F.col("segment") == "SMB", 1).otherwise(0)).alias("smb_count"),
    )
    .orderBy("segment")
)
