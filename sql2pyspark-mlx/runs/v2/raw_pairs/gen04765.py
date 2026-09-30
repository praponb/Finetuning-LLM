from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["FEE", "DEBIT"]) & F.col("account_id").isin([1, 21, 22]))
    .select(
        "account_id",
        "txn_time",
        F.datediff(F.current_date(), "txn_time").alias("days_since_txn_time"),
        F.floor("amount").alias("amount_whole"),
    )
)
