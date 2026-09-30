from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(((F.col("account_type") == "SAVINGS") & (F.col("customer_id") == 13)) | (F.col("balance") > 45000))
    .groupBy("customer_id", "status")
    .agg(F.count("*").alias("total_count"))
    .filter(F.col("total_count") >= 4)
    .orderBy(F.col("total_count").desc(), "customer_id", "status")
    .limit(5)
)
