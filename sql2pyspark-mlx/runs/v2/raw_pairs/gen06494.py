from pyspark.sql import functions as F

recent_accounts = (
    df_accounts
    .filter(((F.col("status") == "CLOSED") & (F.col("balance") > 23000)) | (F.col("balance") > 34000))
    .withColumn("balance_rounded", F.round("balance", 1))
)

summary = (
    recent_accounts
    .groupBy("status", "account_type")
    .agg(F.count("*").alias("total_count"))
)

result = summary.orderBy("total_count", F.col("status").desc())
