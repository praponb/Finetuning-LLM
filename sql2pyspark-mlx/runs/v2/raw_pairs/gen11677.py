from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([3, 30]) & F.col("txn_type").isin(["CREDIT", "FEE"]) & (F.col("txn_type") == "FEE"))
)
