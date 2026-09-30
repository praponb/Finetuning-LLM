from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isin(["Lazada", "Grab", "7-Eleven"]) & (F.col("merchant") != "Shell") & (F.to_date("txn_time") < "2024-10-01"))
    .select(
        "txn_type",
        "amount",
        "is_flagged",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
        F.datediff(F.current_date(), "txn_time").alias("days_since_txn_time"),
    )
)
