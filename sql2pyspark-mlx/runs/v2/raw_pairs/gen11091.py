from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.hour("txn_time").between(7, 11))
    .groupBy("account_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
        F.countDistinct("amount").alias("unique_merchants"),
    )
)

result = (
    df_accounts.alias("accounts")
    .join(account_stats.alias("account_stats"), F.col("accounts.id") == F.col("account_stats.account_id"), "left")
    .filter(F.col("account_stats.unique_merchants") >= 7)
    .select(
        F.col("accounts.opened_date"),
        F.col("account_stats.merchant_values"),
        F.col("account_stats.unique_merchants"),
    )
    .orderBy("opened_date", "merchant_values")
)
