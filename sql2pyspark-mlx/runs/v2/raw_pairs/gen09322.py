from pyspark.sql import functions as F

recent_transactions = df_transactions.filter(F.col("merchant") == "Starbucks")

transactions_stats = (
    recent_transactions
    .groupBy(
        F.when(F.col("amount") >= 4400, "high").when(F.col("amount") >= 600, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.countDistinct("amount").alias("unique_txn_types"))
)

result = (
    transactions_stats
    .filter(F.col("unique_txn_types") > 6)
    .select("amount_bucket", "unique_txn_types")
    .orderBy(F.col("unique_txn_types").desc())
)
