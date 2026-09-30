from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left_semi")
    .filter(F.col("a.opened_date") <= "2024-09-01")
    .select(F.col("a.id"), F.col("a.account_type"))
)
