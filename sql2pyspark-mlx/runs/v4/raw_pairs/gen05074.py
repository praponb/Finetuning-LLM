from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("credit_limit") <= 11300) & F.col("segment").isin(["Consumer", "SMB", "Corporate"]))
    .groupBy(
        F.when(F.col("credit_limit") >= 14600, "high").when(F.col("credit_limit") >= 6300, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(F.max("signup_date").alias("last_signup_date"))
    .orderBy("last_signup_date", "credit_limit_bucket")
)
