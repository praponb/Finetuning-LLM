from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .groupBy("account_id")
    .agg(F.count("*").alias("cnt"))
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("at"), F.col("a.id") == F.col("at.account_id"), "left")
    .select(F.col("a.opened_date"), F.col("at.cnt"))
    .orderBy("cnt", "opened_date")
)
