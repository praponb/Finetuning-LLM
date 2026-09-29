from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["DEBIT", "REFUND"]))
    .select(
        "id",
        "txn_time",
        "merchant",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
    )
)
