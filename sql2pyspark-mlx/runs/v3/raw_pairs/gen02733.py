from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.hour("txn_time").between(11, 18))
    .select(
        "amount",
        "account_id",
        "txn_time",
        F.hour("txn_time").alias("txn_time_hour"),
    )
)
