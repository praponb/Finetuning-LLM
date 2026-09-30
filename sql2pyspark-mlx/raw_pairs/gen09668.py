from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.filter(F.col("amount").between(1300, 2500)).alias("t"), F.col("a.id") == F.col("t.account_id"), "left_semi")
    .filter(F.col("a.balance") < 33000)
    .select(F.col("a.id"), F.col("a.opened_date"), F.col("a.status"))
)
