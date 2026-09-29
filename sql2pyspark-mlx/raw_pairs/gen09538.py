from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .groupBy("account_id")
    .agg(F.count(F.when(F.col("txn_type") == "DEBIT", 1)).alias("debit_count"))
)

result = (
    df_accounts.alias("accounts")
    .join(account_totals.alias("account_totals"), F.col("accounts.id") == F.col("account_totals.account_id"), "left")
    .filter(F.col("account_totals.debit_count") > 9)
    .select(
        F.col("accounts.id"),
        F.col("accounts.opened_date"),
        F.col("account_totals.debit_count"),
    )
    .orderBy(F.col("debit_count").desc(), "id")
)
