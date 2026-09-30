from pyspark.sql import functions as F

result = (
    df_transactions
    .cube("txn_type", "account_id")
    .agg(
        F.countDistinct("amount").alias("unique_txn_types"),
        F.sum(F.when(F.col("txn_type") == "REFUND", 1).otherwise(0)).alias("refund_count"),
        F.round(((100 * F.sum(F.when(F.col("merchant") == "Lazada", 1).otherwise(0))) / F.count("*")), 1).alias("pct_lazada"),
    )
    .orderBy("txn_type")
)
