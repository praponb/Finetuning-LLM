from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("t.amount") < 3500)
    .groupBy(F.col("a.id"))
    .agg(
        F.count(F.col("t.id")).alias("transaction_count"),
        F.max(F.col("t.amount")).alias("max_amount"),
    )
    .orderBy("max_amount", "id")
    .limit(10)
)
