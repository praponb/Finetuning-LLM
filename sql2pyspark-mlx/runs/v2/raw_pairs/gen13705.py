from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "id",
        "txn_time",
        "is_flagged",
        F.hour("txn_time").alias("txn_time_hour"),
        F.concat("txn_type", F.lit(" - "), "merchant").alias("txn_type_merchant"),
    )
)
