from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("account_id") == 16) & F.col("merchant").isin(["Shell", "Lazada"]) & F.col("account_id").isin([21, 32, 35, 40]))
    .select(
        "txn_type",
        F.concat("merchant", F.lit(", "), "txn_type").alias("merchant_txn_type"),
    )
)
