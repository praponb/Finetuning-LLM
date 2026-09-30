from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([24, 31]))
    .select("id", "merchant", "txn_type")
    .unionByName(
        df_transactions
        .filter(F.col("txn_type").isin(["DEBIT", "REFUND", "FEE"]))
        .select("id", "merchant", "txn_type")
    )
    .distinct()
    .orderBy("merchant")
)
