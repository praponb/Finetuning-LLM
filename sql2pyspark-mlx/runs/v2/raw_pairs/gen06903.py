from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "left")
    .groupBy(F.col("a.status"))
    .agg(
        F.count(F.col("t.id")).alias("transaction_count"),
        F.min(F.col("t.amount")).alias("min_amount"),
        F.round(((100 * F.sum(F.when(F.col("t.merchant") == "Lazada", 1).otherwise(0))) / F.count("*")), 1).alias("pct_lazada"),
    )
    .orderBy(F.col("transaction_count").desc())
)
