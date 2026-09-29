from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(~F.col("txn_type").isin(["CREDIT", "REFUND"]))
    .groupBy("account_id")
    .agg(F.sort_array(F.collect_set("merchant")).alias("merchant_list"))
)

result = (
    df_accounts.alias("accounts")
    .join(account_summary.alias("account_summary"), F.col("accounts.id") == F.col("account_summary.account_id"), "left")
    .select(
        F.col("accounts.id"),
        F.col("accounts.account_type"),
        F.col("accounts.customer_id"),
        F.col("account_summary.merchant_list"),
    )
    .orderBy("id")
)
