from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~(F.col("amount").isNotNull() & (F.col("amount") >= 3500) & F.col("account_id").isin([12, 31, 33])))
    .select(
        "id",
        "txn_type",
        "amount",
        "account_id",
        F.trim("txn_type").alias("clean_txn_type"),
        F.coalesce("merchant", F.lit("unassigned")).alias("merchant_filled"),
    )
)
