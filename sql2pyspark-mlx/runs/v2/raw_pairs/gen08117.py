from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("account_id").isin([6, 26, 39]) & F.col("account_id").isin([6, 13, 19, 40])) | F.hour("txn_time").between(10, 16))
    .select("id", "txn_type", "txn_time", "account_id")
    .orderBy(F.col("txn_type").desc())
)
