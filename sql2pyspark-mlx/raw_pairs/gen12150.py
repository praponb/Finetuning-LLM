from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_time") < "2025-08-01")
    .select("txn_type")
    .unionByName(
        df_transactions
        .filter(F.col("txn_type").isin(["DEBIT", "REFUND"]))
        .select("txn_type")
    )
)
