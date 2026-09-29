from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id") == 1)
    .groupBy("merchant")
    .pivot("txn_type", ["CREDIT", "DEBIT", "REFUND"])
    .agg(F.avg("amount"))
)
