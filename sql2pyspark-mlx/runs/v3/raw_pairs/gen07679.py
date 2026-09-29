from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment").isin(["Consumer", "SMB", "Corporate"]))
    .groupBy(F.quarter("signup_date").alias("signup_quarter"))
    .agg(
        F.sum(F.when(F.col("segment") == "SMB", F.col("age")).otherwise(0)).alias("smb_age"),
        F.min("signup_date").alias("first_signup_date"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") >= 1)
    .drop("row_count")
    .orderBy(F.col("first_signup_date").desc())
)
