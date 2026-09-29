from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([4, 9, 15]))
    .select("txn_type", "txn_time", "account_id")
)
