from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["SAVINGS", "LOAN", "CREDIT"]))
    .groupBy("account_type", "status")
    .agg(F.min("balance").alias("min_balance"))
    .filter(F.col("min_balance") >= 39000)
    .select("account_type", "status", "min_balance")
    .orderBy("min_balance", "account_type", "status")
    .limit(5)
)
