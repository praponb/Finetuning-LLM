from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("merchant") == "Lazada") & F.col("txn_type").isin(["REFUND", "CREDIT"]) & F.col("txn_type").isin(["DEBIT", "CREDIT", "REFUND"]))
    .select(
        "id",
        "is_flagged",
        "merchant",
        "account_id",
        F.lower("merchant").alias("merchant_lower"),
    )
    .orderBy("merchant", "id")
    .limit(3)
)
