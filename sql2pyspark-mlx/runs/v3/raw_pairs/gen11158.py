from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isin(["Shell", "Amazon", "7-Eleven"]) | (F.col("amount") > 1300))
    .select(
        "account_id",
        "txn_type",
        "is_flagged",
        F.upper("txn_type").alias("txn_type_upper"),
    )
)
