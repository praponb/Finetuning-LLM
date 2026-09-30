from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country")
    .agg(
        F.sum(F.when(F.col("segment") == "SMB", 1).otherwise(0)).alias("smb_count"),
        F.max("credit_limit").alias("max_credit_limit"),
    )
    .filter(F.col("max_credit_limit") > 15800)
    .select("country", "smb_count", "max_credit_limit")
    .orderBy(F.col("smb_count").desc())
)
