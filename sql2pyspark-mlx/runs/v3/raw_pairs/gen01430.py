from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .select(
        "account_id",
        "txn_time",
        "txn_type",
        F.length("txn_type").alias("txn_type_length"),
    )
)
