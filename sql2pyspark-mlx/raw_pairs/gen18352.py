from pyspark.sql import functions as F

base = (
    df_transactions
    .filter(F.col("account_id").isin([7, 11, 40]) & (F.col("account_id").isin([11, 36]) | F.col("account_id").isin([34, 37, 39, 40])))
)

transactions_stats = (
    base
    .groupBy("account_id", "is_flagged")
    .agg(
        F.count("*").alias("transaction_count"),
        F.countDistinct("amount").alias("unique_accounts"),
    )
)

result = (
    transactions_stats
    .select("account_id", "is_flagged", "transaction_count")
    .orderBy(F.col("transaction_count").desc())
)
