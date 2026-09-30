from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["DEBIT", "FEE", "REFUND"]) & F.col("is_flagged") & (F.col("txn_type") == "FEE"))
    .select(
        "id",
        "txn_time",
        "account_id",
        F.round("amount", 0).alias("amount_rounded"),
    )
    .orderBy(F.col("txn_time").desc(), F.col("amount_rounded").desc())
)
