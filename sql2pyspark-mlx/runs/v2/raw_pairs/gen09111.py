from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .groupBy("account_id")
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
    )
)

result = (
    df_accounts.alias("accounts")
    .join(account_summary.alias("account_summary"), F.col("accounts.id") == F.col("account_summary.account_id"), "inner")
    .filter(F.col("account_summary.amount_range") >= 72)
    .select(
        F.col("accounts.customer_id"),
        F.col("accounts.opened_date"),
        F.col("account_summary.amount_range"),
        F.col("account_summary.merchant_list"),
    )
)
