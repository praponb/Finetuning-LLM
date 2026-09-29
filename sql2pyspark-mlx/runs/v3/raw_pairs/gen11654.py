from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("txn_type").isin(["FEE", "DEBIT"]))
    .groupBy(F.quarter("txn_time").alias("txn_quarter"))
    .agg(
        F.min("txn_time").alias("first_txn_time"),
        F.round(((100 * F.sum(F.when(F.col("merchant") == "Amazon", 1).otherwise(0))) / F.count("*")), 1).alias("pct_amazon"),
        F.avg("amount").alias("avg_amount"),
    )
    .orderBy(F.col("txn_quarter").desc(), F.col("avg_amount").desc())
)
