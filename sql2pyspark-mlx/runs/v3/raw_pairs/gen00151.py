from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.avg("amount").alias("avg_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("T"), F.col("a.id") == F.col("T.account_id"), "left")
    .select(F.col("a.balance"), F.col("T.merchant_list"), F.col("T.avg_amount"))
    .orderBy("balance", F.col("avg_amount").desc())
)
