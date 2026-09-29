from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("txn_time") >= "2023-04-10") & F.col("is_flagged"))
    .groupBy("merchant", "account_id")
    .agg(
        F.max("txn_time").alias("latest_txn_time"),
        F.min("amount").alias("min_amount"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
    .orderBy(F.col("amount_range").desc())
)
