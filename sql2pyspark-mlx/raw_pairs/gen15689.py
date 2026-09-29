from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([5, 19, 21, 26]) & F.col("account_id").isin([1, 3, 16, 33]) & ~F.col("is_flagged"))
    .groupBy("is_flagged")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "CREDIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_credit"),
        F.max("txn_time").alias("latest_txn_time"),
    )
    .orderBy(F.col("pct_credit").desc(), "latest_txn_time", "is_flagged")
    .limit(5)
)
