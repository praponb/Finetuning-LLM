from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") < 2800)
    .select("id", "txn_type", "account_id", "amount")
)
