from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("signup_month"), "segment")
    .agg(
        F.sum(F.when(F.col("city") == "New York", F.col("credit_limit")).otherwise(0)).alias("new_york_credit_limit"),
        F.sum(F.when(F.col("segment") == "SMB", F.col("credit_limit")).otherwise(0)).alias("smb_credit_limit"),
        F.max("signup_date").alias("last_signup_date"),
    )
    .orderBy("signup_month")
)
