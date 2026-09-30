from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount").between(1100, 3800))
    .select("id", "account_id", "txn_type")
)
