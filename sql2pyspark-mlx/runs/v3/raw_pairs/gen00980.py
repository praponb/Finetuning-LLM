from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .groupBy("account_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("merchant") == "Starbucks", 1).otherwise(0))) / F.count("*")), 1).alias("pct_starbucks"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "inner")
    .select(F.col("a.status"), F.col("a.balance"), F.col("tba.pct_starbucks"))
    .orderBy(F.col("balance").desc(), F.col("pct_starbucks").desc())
)
