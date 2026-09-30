from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged") & F.col("txn_type").isin(["REFUND", "CREDIT", "DEBIT"]))
    .groupBy(
        F.when(F.col("amount") >= 3500, "high").when(F.col("amount") >= 2000, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.max("txn_time").alias("last_txn_time"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
)
