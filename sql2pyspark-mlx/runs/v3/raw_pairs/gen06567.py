from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .groupBy(F.col("a.account_type"))
    .agg(
        F.count(F.col("t.id")).alias("transaction_count"),
        F.sum(F.col("t.amount")).alias("total_amount"),
        F.avg(F.col("t.amount")).alias("avg_amount"),
    )
    .orderBy("account_type")
)
