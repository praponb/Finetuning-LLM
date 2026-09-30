from pyspark.sql import functions as F

eligible = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") < F.col("s.opened_date")), "left_semi")
)

result = (
    eligible
    .select("id", "balance")
    .orderBy("balance", F.col("id").desc())
)
