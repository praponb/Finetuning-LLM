from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .groupBy(F.col("a.id"), F.col("a.status"))
    .agg(
        F.count(F.col("t.id")).alias("transaction_count"),
        F.min(F.col("t.amount")).alias("min_amount"),
        F.round(F.avg(F.col("t.amount")), 2).alias("avg_amount"),
    )
    .orderBy(F.col("min_amount").desc(), "id", "status")
    .limit(5)
)
