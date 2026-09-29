from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.col("a.status").isNull())
    .groupBy(
        F.col("a.account_type"),
        F.year(F.col("a.opened_date")).alias("opened_year"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("t.merchant") == "Amazon", 1).otherwise(0))) / F.count("*")), 1).alias("pct_amazon"),
        F.sum(F.when(F.col("t.txn_type") == "DEBIT", 1).otherwise(0)).alias("debit_count"),
    )
    .filter(F.col("debit_count") > 58)
)
