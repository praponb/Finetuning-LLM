from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .groupBy(F.col("a.status"))
    .agg(
        F.sum(F.col("t.amount")).alias("total_amount"),
        F.round(F.avg(F.col("t.amount")), 2).alias("avg_amount"),
    )
    .orderBy(F.col("total_amount").desc(), "status")
    .limit(5)
)
