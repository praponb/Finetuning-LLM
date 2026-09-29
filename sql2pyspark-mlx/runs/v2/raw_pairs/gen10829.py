from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status") != "CLOSED")
    .groupBy("status", "account_type", "customer_id")
    .agg(F.count(F.when(F.col("account_type") == "LOAN", 1)).alias("loan_count"))
    .orderBy("account_type", F.col("customer_id").desc())
)
