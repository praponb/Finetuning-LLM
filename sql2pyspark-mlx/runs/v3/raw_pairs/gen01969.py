from pyspark.sql import functions as F

src = (
    df_transactions
    .filter((F.col("txn_type") == "REFUND") | F.col("account_id").isin([1, 15]))
)

txn_type_summary = (
    src
    .groupBy("txn_type")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "FEE", 1).otherwise(0))) / F.count("*")), 1).alias("pct_fee"),
        F.count(F.when(F.col("txn_type") == "REFUND", 1)).alias("refund_count"),
    )
)

result = txn_type_summary
