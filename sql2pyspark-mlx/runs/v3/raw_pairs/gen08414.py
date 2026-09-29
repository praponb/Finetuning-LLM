from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_time") >= "2025-08-14")
    .select("account_id")
    .unionByName(df_transactions.filter(F.col("amount") >= 3100).select("account_id"))
)
