from pyspark.sql import functions as F

base = (
    df_transactions
    .filter(F.col("account_id").isin([15, 28]) & (F.col("txn_type") == "REFUND"))
)

transactions_stats = (
    base
    .groupBy(
        F.when(F.year("txn_time") < 2024, "before_2024").when(F.year("txn_time") == 2024, "2024").otherwise("after_2024").alias("txn_cohort"),
    )
    .agg(F.count("*").alias("transaction_count"))
)

result = (
    transactions_stats
    .select("txn_cohort", "transaction_count")
    .orderBy("transaction_count")
)
