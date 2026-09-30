from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.hour("txn_time").between(12, 18) & F.col("account_id").isin([19, 21]) & F.col("account_id").isin([3, 19]))
    .select(
        "id",
        "txn_time",
        F.hour("txn_time").alias("txn_time_hour"),
        F.round("amount", 0).alias("amount_rounded"),
    )
)
