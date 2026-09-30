from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id") == 26)
    .select("id", "txn_type", "txn_time", "is_flagged")
    .orderBy("txn_time", "txn_type")
)
