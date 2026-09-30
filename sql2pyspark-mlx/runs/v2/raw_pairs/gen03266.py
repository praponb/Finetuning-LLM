from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.col("a.status") == "OPEN")
    .groupBy(F.col("a.id"))
    .agg(
        F.count(F.col("t.id")).alias("transaction_count"),
        F.sum(F.col("t.amount")).alias("total_amount"),
    )
    .orderBy(F.col("id").desc())
)
