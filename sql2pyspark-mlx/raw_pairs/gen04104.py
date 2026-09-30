from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .groupBy(
        F.when(F.col("a.balance") >= 84000, "high").when(F.col("a.balance") >= 18000, "mid").otherwise("low").alias("balance_bucket"),
        F.date_trunc("MONTH", F.col("a.opened_date")).alias("opened_month"),
    )
    .agg(F.round(F.stddev(F.col("t.amount")), 2).alias("stddev_amount"))
)
