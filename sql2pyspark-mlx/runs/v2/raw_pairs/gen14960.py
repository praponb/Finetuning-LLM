from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~(F.col("txn_type").isin(["DEBIT", "CREDIT", "REFUND"]) & F.col("merchant").isNull() & F.col("account_id").isin([1, 12, 25, 33])))
    .select("id", "account_id")
)
