from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("txn_time") >= "2024-10-13") & F.col("amount").isNotNull())
    .select("account_id", F.trim("merchant").alias("clean_merchant"))
    .orderBy("account_id")
)
