from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([6, 39]))
    .select("txn_type", "account_id", "merchant")
)
