from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["DEBIT", "REFUND"]))
    .select("id", "merchant", "amount")
)
