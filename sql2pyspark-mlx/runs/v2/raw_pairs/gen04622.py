from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "outer")
    .groupBy(F.col("a.status"))
    .agg(
        F.count(F.col("t.amount")).alias("amount_count"),
        F.max(F.col("t.amount")).alias("max_amount"),
    )
    .filter(F.col("amount_count") > 3)
    .orderBy("max_amount")
)
