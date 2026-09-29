from pyspark.sql import functions as F

src = (
    df_transactions
    .filter((F.col("txn_type") != "CREDIT") & F.col("account_id").isin([25, 34, 38, 40]))
)

transactions_stats = (
    src
    .groupBy("merchant")
    .agg(F.min("txn_time").alias("first_txn_time"))
)

result = (
    transactions_stats
    .select("merchant", "first_txn_time")
    .orderBy("merchant")
)
