from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("amount") >= 3100) & (F.col("amount") < 3300))
    .select(
        "id",
        "account_id",
        "txn_time",
        "merchant",
        F.quarter("txn_time").alias("txn_time_quarter"),
    )
    .orderBy(F.col("merchant").desc())
)
