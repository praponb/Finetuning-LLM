from pyspark.sql import functions as F

valid_customers = df_customers.filter(F.col("age") <= 41)

segment_summary = (
    valid_customers
    .groupBy("segment", F.quarter("signup_date").alias("signup_quarter"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("segment") == "SMB", 1).otherwise(0))) / F.count("*")), 1).alias("pct_smb"),
    )
)

result = segment_summary
