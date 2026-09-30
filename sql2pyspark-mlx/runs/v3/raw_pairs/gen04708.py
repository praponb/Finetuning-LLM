from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged") & F.hour("txn_time").between(7, 12) & (F.col("txn_time") >= "2025-09-24"))
    .select("id", "merchant", "account_id", "txn_type")
)
