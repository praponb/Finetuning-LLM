from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .groupBy("account_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .select(F.col("a.balance"), F.col("a.customer_id"), F.col("t.merchant_values"))
)
