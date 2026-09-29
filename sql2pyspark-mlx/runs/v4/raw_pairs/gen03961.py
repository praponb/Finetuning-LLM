from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.col("account_id").isin([16, 24, 40]))
    .groupBy("account_id")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.count("*").alias("num_transactions"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("A"), F.col("a.id") == F.col("A.account_id"), "left")
    .filter(F.col("A.avg_amount") > 50)
    .select(
        F.col("a.id"),
        F.col("a.customer_id"),
        F.col("A.avg_amount"),
        F.col("A.num_transactions"),
    )
    .orderBy("num_transactions")
)
