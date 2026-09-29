from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .groupBy(
        F.when(F.col("a.balance") >= 60000, "high").when(F.col("a.balance") >= 34000, "mid").otherwise("low").alias("balance_bucket"),
        F.date_trunc("MONTH", F.col("a.opened_date")).alias("opened_month"),
    )
    .agg(F.max(F.col("t.txn_time")).alias("latest_txn_time"))
    .orderBy("latest_txn_time", F.col("opened_month").desc())
)
