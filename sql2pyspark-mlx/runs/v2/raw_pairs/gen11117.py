from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .groupBy(F.col("a.id"))
    .agg(
        F.count(F.col("t.id")).alias("transaction_count"),
        F.sum(F.col("t.amount")).alias("total_amount"),
    )
    .orderBy("id", F.col("total_amount").desc())
    .limit(3)
)
