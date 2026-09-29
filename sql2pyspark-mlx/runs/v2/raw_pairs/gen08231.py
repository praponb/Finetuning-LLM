from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("amount") >= 3000) & F.col("txn_type").isin(["REFUND", "CREDIT"]) & ~F.col("is_flagged"))
    .select(
        "amount",
        F.concat("merchant", F.lit(", "), "txn_type").alias("merchant_txn_type"),
        F.lower("txn_type").alias("txn_type_lower"),
    )
    .orderBy("txn_type_lower")
)
