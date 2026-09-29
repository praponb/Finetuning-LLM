from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .groupBy("account_id")
    .agg(F.count(F.when(F.col("txn_type") == "CREDIT", 1)).alias("credit_count"))
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("at"), F.col("a.id") == F.col("at.account_id"), "left")
    .select(F.col("a.opened_date"), F.col("a.account_type"), F.col("at.credit_count"))
    .orderBy(F.col("credit_count").desc(), "account_type")
)
