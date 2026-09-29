from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("merchant") == "Starbucks") & F.col("account_id").isin([9, 12]))
    .select("id", "account_id", "txn_type")
)
