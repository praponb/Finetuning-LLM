from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("txn_time") >= "2024-02-01") & ((F.col("amount") >= 2200) | (F.col("txn_type") == "FEE")))
    .groupBy("merchant")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "REFUND", 1).otherwise(0))) / F.count("*")), 1).alias("pct_refund"),
    )
)
