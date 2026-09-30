from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("account_id")
    .pivot("txn_type", ["DEBIT", "CREDIT"])
    .agg(F.avg("amount"))
)
