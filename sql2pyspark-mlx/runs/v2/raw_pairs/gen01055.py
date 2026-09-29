from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("amount") <= 2800) & (F.col("txn_type").isin(["REFUND", "DEBIT", "FEE"]) | (F.col("account_id") == 36)))
    .select("id", "account_id", "txn_type", "txn_time")
    .orderBy(F.col("txn_type").desc())
)
