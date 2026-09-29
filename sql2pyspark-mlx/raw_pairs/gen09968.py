from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "id",
        "is_flagged",
        "amount",
        "account_id",
        F.hour("txn_time").alias("txn_time_hour"),
    )
)
