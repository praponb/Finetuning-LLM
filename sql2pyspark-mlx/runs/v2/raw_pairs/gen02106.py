from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .groupBy("account_id")
    .agg(F.count("*").alias("num_transactions"))
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .select(
        F.col("a.id"),
        F.col("a.customer_id"),
        F.col("a.opened_date"),
        F.col("t.num_transactions"),
    )
    .orderBy(F.col("num_transactions").desc())
)
