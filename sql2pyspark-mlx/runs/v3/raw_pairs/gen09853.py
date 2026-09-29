from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left_semi")
    .filter(F.col("a.opened_date") < "2025-02-01")
    .select(F.col("a.id"), F.col("a.balance"))
    .orderBy("balance")
)
