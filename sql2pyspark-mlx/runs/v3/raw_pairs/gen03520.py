from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("amount") < 1100) & (F.col("amount") >= 2800))
    .select("id", "txn_time", "account_id", "merchant")
)
