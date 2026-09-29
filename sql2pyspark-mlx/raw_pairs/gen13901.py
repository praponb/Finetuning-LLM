from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.max("txn_time").alias("last_txn_time"),
        F.round(((100 * F.sum(F.when(F.col("merchant") == "Amazon", 1).otherwise(0))) / F.count("*")), 1).alias("pct_amazon"),
    )
    .orderBy("pct_amazon")
)
