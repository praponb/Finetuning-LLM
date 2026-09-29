from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("age") >= 61, "high").when(F.col("age") >= 34, "mid").otherwise("low").alias("age_bucket"),
        F.date_trunc("MONTH", "signup_date").alias("signup_month"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("segment") == "SMB", 1).otherwise(0))) / F.count("*")), 1).alias("pct_smb"),
        F.min("age").alias("min_age"),
    )
)
