from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("t.account_id").isin([13, 14, 15, 24]))
    .groupBy(F.col("a.id"), F.col("a.status"))
    .agg(
        F.count(F.col("t.id")).alias("transaction_count"),
        F.round(F.avg(F.col("t.amount")), 2).alias("avg_amount"),
        F.count(F.col("t.amount")).alias("amount_count"),
    )
    .orderBy(F.col("id").desc())
)
