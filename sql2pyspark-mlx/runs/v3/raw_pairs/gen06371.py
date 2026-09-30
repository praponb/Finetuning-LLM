from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["FEE", "DEBIT"]))
    .select("id", "merchant", "amount", "txn_type")
)
