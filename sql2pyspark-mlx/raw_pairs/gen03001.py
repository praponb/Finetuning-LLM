from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .groupBy("account_id")
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)

result = (
    df_accounts.alias("accounts")
    .join(account_totals.alias("account_totals"), F.col("accounts.id") == F.col("account_totals.account_id"), "inner")
    .filter(F.col("account_totals.avg_amount") > 26)
    .select(
        F.col("accounts.opened_date"),
        F.col("accounts.account_type"),
        F.col("account_totals.avg_amount"),
    )
    .orderBy("account_type")
)
