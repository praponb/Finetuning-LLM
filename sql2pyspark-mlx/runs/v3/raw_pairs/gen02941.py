from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "id",
        "txn_type",
        "is_flagged",
        "txn_time",
        F.hour("txn_time").alias("txn_time_hour"),
    )
)
