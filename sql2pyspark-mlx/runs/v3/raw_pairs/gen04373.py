from pyspark.sql import functions as F

valid_transactions = (
    df_transactions
    .filter(((F.col("amount") <= 3200) & F.col("amount").between(1300, 3800)) | (F.col("account_id") == 21))
)

transactions_stats = (
    valid_transactions
    .groupBy("txn_type")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.count("amount").alias("amount_count"),
    )
)

result = (
    transactions_stats
    .filter(F.col("stddev_amount") < 73)
    .orderBy("amount_count", "txn_type")
    .limit(10)
)
