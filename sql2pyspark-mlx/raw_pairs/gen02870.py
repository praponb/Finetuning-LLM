from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~((F.col("txn_type") == "DEBIT") & (F.col("amount") <= 2100)))
    .select("id", "txn_type", "merchant")
)
