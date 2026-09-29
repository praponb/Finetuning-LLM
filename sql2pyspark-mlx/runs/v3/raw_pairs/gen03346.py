from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("txn_time") >= "2024-03-23") & ((F.col("amount") >= 3100) | F.col("is_flagged")))
    .select(
        "id",
        "account_id",
        "txn_type",
        "merchant",
        F.col("txn_time").cast("date").alias("txn_time_day"),
    )
    .orderBy(F.col("txn_time_day").desc())
)
