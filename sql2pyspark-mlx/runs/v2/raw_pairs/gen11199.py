from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country") == "TH")
    .groupBy("is_active")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("segment") == "SMB", 1).otherwise(0))) / F.count("*")), 1).alias("pct_smb"),
    )
    .orderBy(F.col("pct_smb").desc())
)
