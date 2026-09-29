from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .groupBy(F.col("a.status"), F.col("a.account_type"))
    .agg(
        F.avg(F.col("t.amount")).alias("avg_amount"),
        F.round(((100 * F.sum(F.when(F.col("t.txn_type") == "CREDIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_credit"),
    )
    .orderBy(F.col("status").desc(), "account_type")
    .limit(50)
)
