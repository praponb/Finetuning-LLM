from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([2, 10, 21]) & (F.col("account_id") == 22))
    .select("txn_type")
)
