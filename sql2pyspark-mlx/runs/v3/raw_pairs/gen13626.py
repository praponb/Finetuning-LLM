from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("email").like("%@gmail.com") & F.col("segment").isNull()) | F.col("email").isNotNull())
    .groupBy("city", F.date_format("signup_date", "yyyy-MM").alias("signup_month"))
    .agg(F.sum(F.when(F.col("segment") == "SMB", 1).otherwise(0)).alias("smb_count"))
)
