from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .select(
        "id",
        "txn_time",
        F.datediff(F.current_date(), "txn_time").alias("days_since_txn_time"),
        F.floor("amount").alias("amount_whole"),
    )
)
