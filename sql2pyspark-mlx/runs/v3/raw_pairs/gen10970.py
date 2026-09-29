from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .filter(F.to_date("txn_time") >= "2023-10-01")
    .groupBy("account_id")
    .agg(F.count("*").alias("cnt"))
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("at"), F.col("a.id") == F.col("at.account_id"), "inner")
    .filter(F.col("at.cnt") > 3)
    .select(F.col("a.account_type"), F.col("at.cnt"))
)
