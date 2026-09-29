from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type") == "REFUND")
    .select(
        "id",
        "account_id",
        "txn_type",
        F.weekofyear("txn_time").alias("txn_time_weekofyear"),
    )
)
