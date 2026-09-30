from pyspark.sql import functions as F

recent_transactions = (
    df_transactions
    .filter((F.col("account_id") == 13) & F.col("account_id").isin([16, 17, 23]))
)

transactions_stats = (
    recent_transactions
    .groupBy("is_flagged")
    .agg(F.countDistinct("amount").alias("unique_accounts"))
)

result = (
    transactions_stats
    .select("is_flagged", "unique_accounts")
    .orderBy("unique_accounts")
)
