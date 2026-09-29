from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("a.account_type").isin(["SAVINGS", "CHECKING", "CREDIT"]))
    .groupBy(F.col("a.account_type"))
    .agg(
        F.count(F.col("t.id")).alias("transaction_count"),
        F.round(F.stddev(F.col("t.amount")), 2).alias("stddev_amount"),
        F.avg(F.col("t.amount")).alias("avg_amount"),
    )
)
