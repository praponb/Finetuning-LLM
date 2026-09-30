from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant") != "7-Eleven")
    .select(
        "id",
        "account_id",
        "txn_type",
        F.col("txn_time").cast("date").alias("txn_time_day"),
    )
)
