from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .groupBy("account_id")
    .agg(F.sort_array(F.collect_set("merchant")).alias("merchant_list"))
)

result = (
    df_accounts.alias("accounts")
    .join(account_summary.alias("t"), F.col("accounts.id") == F.col("t.account_id"), "inner")
    .select(
        F.col("accounts.id"),
        F.col("accounts.opened_date"),
        F.col("t.merchant_list"),
    )
)
