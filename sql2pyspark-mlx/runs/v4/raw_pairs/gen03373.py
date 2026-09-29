from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("txn_type").isin(["CREDIT", "FEE", "REFUND"]) & F.col("account_id").isin([13, 15]) & (F.col("account_id") == 20))
    .select(
        "id",
        "is_flagged",
        "merchant",
        "amount",
        F.datediff(F.current_date(), "txn_time").alias("days_since_txn_time"),
    )
    .orderBy("merchant")
)
