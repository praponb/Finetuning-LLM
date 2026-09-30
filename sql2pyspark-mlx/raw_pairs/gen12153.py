from pyspark.sql import functions as F

eligible = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.amount") > 2400), "left_anti")
)

result = (
    eligible
    .select("id", "balance", "opened_date")
    .orderBy("opened_date", "balance")
)
