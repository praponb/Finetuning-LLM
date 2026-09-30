from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.to_date("txn_time") >= "2025-06-27")
    .groupBy("account_id")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.countDistinct("amount").alias("unique_accounts"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(F.col("t.unique_accounts") >= 4)
    .select(
        F.col("a.status"),
        F.col("a.account_type"),
        F.col("t.avg_amount"),
        F.col("t.unique_accounts"),
    )
    .orderBy(F.col("unique_accounts").desc())
)
