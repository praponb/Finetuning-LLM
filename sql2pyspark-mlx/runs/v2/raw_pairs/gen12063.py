from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("merchant") == "Amazon", 1).otherwise(0))) / F.count("*")), 1).alias("pct_amazon"),
        F.countDistinct("amount").alias("unique_merchants"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "inner")
    .filter(F.col("tba.pct_amazon") >= 59)
    .select(F.col("a.balance"), F.col("tba.pct_amazon"), F.col("tba.unique_merchants"))
    .orderBy(F.col("pct_amazon").desc())
)
