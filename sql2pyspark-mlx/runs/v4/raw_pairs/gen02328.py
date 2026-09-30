from pyspark.sql import functions as F

result = (
    df_customers
    .filter(((F.col("credit_limit") > 15900) & F.col("segment").isNotNull()) | F.col("age").isNotNull())
    .groupBy(
        F.when(F.col("credit_limit") >= 16700, "high").when(F.col("credit_limit") >= 4400, "mid").otherwise("low").alias("credit_limit_bucket"),
        "segment",
        F.date_trunc("MONTH", "signup_date").alias("signup_month"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("segment") == "SMB", 1).otherwise(0))) / F.count("*")), 1).alias("pct_smb"),
    )
    .orderBy("signup_month")
)
