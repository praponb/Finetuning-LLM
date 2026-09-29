from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([11, 18, 22, 38]) & F.col("is_flagged"))
    .select(
        "id",
        "merchant",
        "txn_type",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
    )
)
