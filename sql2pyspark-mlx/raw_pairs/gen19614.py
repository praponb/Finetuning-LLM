from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("txn_time") < "2023-08-16") & (~F.col("merchant").isin(["Starbucks", "Grab"]) | (F.col("account_id") == 13)))
    .groupBy("is_flagged")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("merchant") == "Amazon", 1).otherwise(0))) / F.count("*")), 1).alias("pct_amazon"),
        F.min("txn_time").alias("min_txn_time"),
    )
)
