from pyspark.sql import functions as F

result = (
    df_transactions
    .rollup("account_id", "merchant")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("merchant") == "Lazada", 1).otherwise(0))) / F.count("*")), 1).alias("pct_lazada"),
        F.count("amount").alias("amount_count"),
        F.min("txn_time").alias("min_txn_time"),
    )
    .orderBy("pct_lazada", "amount_count")
)
