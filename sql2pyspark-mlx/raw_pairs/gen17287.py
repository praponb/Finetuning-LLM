from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("t.merchant").isin(["Lazada", "Shell", "7-Eleven"]))
    .groupBy(F.col("a.id"), F.col("a.account_type"))
    .agg(
        F.count(F.col("t.id")).alias("transaction_count"),
        F.round(F.avg(F.col("t.amount")), 2).alias("avg_amount"),
        F.sum(F.col("t.amount")).alias("total_amount"),
    )
    .orderBy(F.col("transaction_count").desc())
)
