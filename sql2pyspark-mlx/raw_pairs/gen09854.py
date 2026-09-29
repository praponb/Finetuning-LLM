from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id") == 33)
    .select("id", "txn_time", "txn_type")
    .orderBy("txn_time", "id")
    .limit(1)
)
