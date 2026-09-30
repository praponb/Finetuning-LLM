from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") <= 2200)
    .select(
        "id",
        "merchant",
        "account_id",
        "txn_time",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
    )
)
