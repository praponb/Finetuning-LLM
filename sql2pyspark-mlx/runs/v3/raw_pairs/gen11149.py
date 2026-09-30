from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .select(
        "id",
        "txn_time",
        "amount",
        "is_flagged",
        F.hour("txn_time").alias("txn_time_hour"),
    )
)
