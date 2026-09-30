from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.month("txn_time").alias("txn_month"), "txn_type")
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.max("txn_time").alias("last_txn_time"),
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "FEE", 1).otherwise(0))) / F.count("*")), 1).alias("pct_fee"),
    )
    .filter(F.col("amount_range") < 49)
    .orderBy(F.col("last_txn_time").desc())
)
