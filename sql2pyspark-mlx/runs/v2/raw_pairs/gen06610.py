from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.alias("transactions"), F.col("accounts.id") == F.col("transactions.account_id"), "left")
    .filter(F.month(F.col("accounts.opened_date")).isin([2, 6, 9]))
    .groupBy(F.col("accounts.account_type"))
    .agg(
        F.count(F.col("transactions.id")).alias("transaction_count"),
        F.sum(F.when(F.col("transactions.merchant") == "Amazon", F.col("transactions.amount")).otherwise(0)).alias("amazon_amount"),
    )
    .orderBy(F.col("amazon_amount").desc())
)
