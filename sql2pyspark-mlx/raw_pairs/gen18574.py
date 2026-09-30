from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("is_flagged") & (F.col("txn_type") != "DEBIT") & (F.col("account_id") == 32))
    .select("merchant", "account_id", "txn_time")
)
