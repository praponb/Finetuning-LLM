from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged") | (F.col("txn_type") == "CREDIT"))
    .select(
        "id",
        "account_id",
        "merchant",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
        F.trim("txn_type").alias("clean_txn_type"),
    )
)
