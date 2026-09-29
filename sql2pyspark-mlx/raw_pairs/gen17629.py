from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.col("amount") <= 2400)
    .groupBy("account_id")
    .agg(
        F.count("amount").alias("amount_count"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .select(F.col("a.opened_date"), F.col("t.amount_count"), F.col("t.merchant_list"))
)
