from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("merchant") == "Grab") & F.col("txn_type").isin(["FEE", "DEBIT"]))
    .select("id", "txn_time", "merchant", "is_flagged")
)
