from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment").isin(["SMB", "Corporate"]) & F.col("city").isin(["Berlin", "New York", "Bangkok"]) & (F.col("signup_date") <= "2023-05-01"))
    .groupBy("city")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("segment") == "SMB", 1).otherwise(0))) / F.count("*")), 1).alias("pct_smb"),
        F.sum(F.when(F.col("country") == "TH", 1).otherwise(0)).alias("th_count"),
    )
)
