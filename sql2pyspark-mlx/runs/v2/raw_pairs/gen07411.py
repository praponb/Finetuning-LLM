from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city").isin(["London", "Bangkok"]))
    .groupBy("is_active", F.quarter("signup_date").alias("signup_quarter"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("segment") == "SMB", 1).otherwise(0))) / F.count("*")), 1).alias("pct_smb"),
    )
    .filter(F.col("pct_smb") > 25)
    .orderBy(F.col("signup_quarter").desc())
)
