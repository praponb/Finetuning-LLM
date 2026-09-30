from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("t.is_flagged"))
    .groupBy(F.col("a.id"))
    .agg(
        F.count(F.col("t.id")).alias("transaction_count"),
        F.sum(F.col("t.amount")).alias("total_amount"),
    )
    .filter(F.col("transaction_count") >= 2)
    .orderBy(F.col("transaction_count").desc(), F.col("total_amount").desc())
)
