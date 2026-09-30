from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "id",
        "account_id",
        "txn_time",
        "merchant",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
        F.weekofyear("txn_time").alias("txn_time_weekofyear"),
    )
)
